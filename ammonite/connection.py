import pika
import time
from ammonite.utils import logger

MAX_RETRIES = 8


class Consumer(object):
    def __init__(self, config):
        self.username = config.get('AMQP', 'USER')
        self.password = config.get('AMQP', 'PASSWORD')
        self.hostname = config.get('AMQP', 'HOSTNAME')
        self.queue_name = config.get('QUEUES', 'JOBS')
        self.slots = int(config.get('WORKER', 'SLOTS'))
        self.config = config

    def connect(self):
        credentials = pika.PlainCredentials(self.username,
                                            self.password,)
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

    def consume(self, handler):
        connection = self.get_connection()
        channel = connection.channel()

        queue_name = self.queue_name

        channel.queue_declare(queue=queue_name, durable=True)
        logger.info('---')
        logger.info('--- Ammonite worker ready.')
        logger.info('--- Waiting for messages.')
        logger.info('--- To exit press CTRL+C')
        logger.info('---')

        channel.basic_qos(prefetch_count=int(self.slots))
        channel.basic_consume(handler(self.config),
                              queue=queue_name)
        channel.start_consuming()
