from contextlib import contextmanager
import pika
import time
import json
import logging
from threading import Thread
from ammonite.utils import SENTRY_CLIENT

MAX_RETRIES = 8

logger = logging.getLogger('ammonite.connection')
pika_logger = logging.getLogger('pika')
pika_logger.setLevel(logging.CRITICAL)


class Base(object):
    def __init__(self, queue_name, config):
        self.username = config.get('AMQP', 'USER')
        self.password = config.get('AMQP', 'PASSWORD')
        self.hostname = config.get('AMQP', 'HOSTNAME')
        self.queue_name = queue_name
        self.slots = 1

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


class Receiver(Base):
    def threaded_listen(self, handler, broadcast=False):
        thread = Thread(target=self.listen,
                        args=(handler, broadcast))
        thread.daemon = True
        thread.start()

    def listen(self, handler, broadcast=False):
        connection = self.get_connection()
        channel = connection.channel()

        queue_name = self.queue_name
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

        channel.basic_consume(handler,
                              queue=queue_name)
        channel.start_consuming()


class Sender(Base):
    @contextmanager
    def open_channel(self):
        connection = self.get_connection()
        channel = connection.channel()
        yield channel
        connection.close()

    def send(self, message, queue_name=None):
        queue_name = queue_name or self.queue_name
        if not isinstance(message, str):
            message = json.dumps(message)
        with self.open_channel() as channel:
            logger.info('--- Sending message to channel %s.' % queue_name)
            channel.queue_declare(queue=queue_name, durable=True)
            properties = pika.BasicProperties(delivery_mode=2,)
            channel.basic_publish(exchange='',
                                  routing_key=queue_name,
                                  body=message,
                                  properties=properties)

    def broadcast(self, message, exchange_name=None):
        if not isinstance(message, str):
            message = json.dumps(message)
        exchange_name = exchange_name or self.queue_name
        with self.open_channel() as channel:
            channel.exchange_declare(exchange=exchange_name,
                                     type='fanout')
            channel.basic_publish(exchange=exchange_name,
                                  routing_key='',
                                  body=message)


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
