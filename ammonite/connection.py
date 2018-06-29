from contextlib import contextmanager
import pika
import time
import json
import logging
from threading import Thread
from ammonite.utils import SENTRY_CLIENT, logger

MAX_RETRIES = 8

pika_logger = logging.getLogger('pika')
pika_logger.setLevel(logging.CRITICAL)


class Base(object):
    def __init__(self, queue_name, config):
        self.slots = 1
        self.queue_name = queue_name
        self.config = config
        self.username = config.get('AMQP', 'USER')
        self.password = config.get('AMQP', 'PASSWORD')
        self.hostname = config.get('AMQP', 'HOSTNAME')
        self.connection = self.get_connection()

    def connect(self):
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(host=self.hostname,
                                               credentials=credentials)
        return pika.BlockingConnection(parameters)

    def get_connection(self):
        retry = 1
        connection = None
        while retry <= MAX_RETRIES:
            timeout = retry ** 2
            try:
                connection = self.connect()
                # connected, break the while
                break
            except pika.exceptions.AMQPConnectionError:
                time.sleep(timeout)
                retry += 1
                if retry > MAX_RETRIES:
                    raise
                logger.error("Could not connect. Retrying in %ss" % timeout)
        return connection

    def close(self):
        self.connection.close()


class Receiver(Base):
    def threaded_listen(self, handler, broadcast=False):
        thread = Thread(target=self.listen, args=(handler, broadcast))
        thread.start()

    def listen(self, *args, **kwargs):
        try:
            self._listen(*args, **kwargs)
        except pika.exceptions.ConnectionClosed:
            logger.warning("Resetting connection")
            self.connection = self.get_connection()
            self.listen(*args, **kwargs)

    def _listen(self, handler, broadcast=False):
        logger.info("Setting up connection")
        queue_name = self.queue_name
        channel = self.connection.channel()
        if broadcast:
            channel.exchange_declare(exchange=self.queue_name,
                                     type='fanout')
            result = channel.queue_declare(exclusive=True)
            queue_name = result.method.queue
            channel.queue_bind(exchange=self.queue_name,
                               queue=queue_name)
        else:
            if not queue_name:
                raise Exception("non broadcast consumes need a queue name")
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_qos(prefetch_count=int(self.slots))

        channel.receiver = self
        channel.basic_consume(handler, queue=queue_name)
        channel.start_consuming()


class Sender(Base):
    def __init__(self, broadcast=False, *args, **kwargs):
        super().__init__(*args, **kwargs)

        logger.info('*** Broadcasting channel "%s": %s' % (self.queue_name,
                                                           broadcast))
        self.broadcast = broadcast

    def send(self, message):
        if not isinstance(message, str):
            message = json.dumps(message)

        params = {'exchange': '',
                  'routing_key': ''}

        try:
            channel = self.connection.channel()
        except Exception:
            logger.info('Reconnecting')
            self.connection = self.get_connection()
            channel = self.connection.channel()

        if self.broadcast:
            channel.exchange_declare(exchange=self.queue_name,
                                     type='fanout')
            params['exchange'] = self.queue_name
        else:
            channel.queue_declare(queue=self.queue_name, durable=True)
            params['routing_key'] = self.queue_name
            params['properties'] = pika.BasicProperties(delivery_mode=2,)
        channel.basic_publish(body=message, **params)

    def close(self):
        self.connection.close()


class BaseHandler(object):
    def __call__(self, ch, method, properties, body):
        try:
            recipe = json.loads(body.decode('utf-8'))
            self.call(recipe)
        except Exception as e:
            logger.critical("Exception: %s" % e)
            if SENTRY_CLIENT:
                SENTRY_CLIENT.captureException()
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def call(self, recipe):
        raise NotImplementedError()
