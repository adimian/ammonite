import pika
import argparse
import ConfigParser
import os
import logging
import sys
import tempfile
import json
import docker
logging.basicConfig(stream=sys.stdout,
                    format="%(asctime)s [%(levelname)s] %(message)s")

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG


class ExecutionCallback(object):

    def __init__(self, connection, config):
        self.connection = connection
        self.config = config

    def get_docker_client(self):
        client = docker.Client(base_url=self.config.get('DOCKER', 'ENDPOINT'))
        return client

    def put_in_message_queue(self, queue, message):
        channel = self.connection.channel()
        channel.queue_declare(queue=queue, durable=True)

        properties = pika.BasicProperties(delivery_mode=2,)
        channel.basic_publish(exchange='',
                              routing_key=queue,
                              body=message,
                              properties=properties)

    def _create_temp_dir(self, direction):
        return tempfile.mktemp(prefix='ammonite-%s-' % direction)

    def create_inbox(self):
        return self._create_temp_dir(direction='inbox')

    def create_outbox(self):
        return self._create_temp_dir(direction='outbox')

    def __call__(self, ch, method, properties, body):
        logger.info('received %r', body)
        logger.info('starting to work')

        recipe = json.loads(body)

        inbox = self.create_inbox()
        outbox = self.create_outbox()

        docker = self.get_docker_client()
        image_name = '/'.join((self.config.get('DOCKER', 'REGISTRY'),
                               recipe['image']))
        container = docker.create_container(image=image_name,
                                            command=recipe['command'],
                                            volumes=[inbox,
                                                     outbox])

        response = docker.start(container=container.get('Id'),
                                binds={inbox: {'bind': '/inbox',
                                               'ro': True},
                                       outbox: {'bind': '/outbox',
                                                'ro': False}})

        self.put_in_message_queue(queue='results',
                                  message=json.dumps({'id': recipe['execution'],
                                                      'outbox': outbox,
                                                      'state': 'done',
                                                      'response': response,
                                                      'cpu': 0,
                                                      'memory': 0,
                                                      'io': 0}))

        logger.info('done working')
        ch.basic_ack(delivery_tag=method.delivery_tag)


def serve(config):
    parameters = pika.ConnectionParameters(host=config.get('AMQP', 'HOSTNAME'))
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    queue_name = config.get('QUEUES', 'JOBS')

    channel.queue_declare(queue=queue_name, durable=True)
    logger.info('---')
    logger.info('--- Ammonite worker ready.')
    logger.info('--- Waiting for messages.')
    logger.info('--- To exit press CTRL+C')
    logger.info('---')

    channel.basic_qos(prefetch_count=int(config.get('WORKER', 'SLOTS')))
    channel.basic_consume(ExecutionCallback(connection, config),
                          queue=queue_name)

    channel.start_consuming()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--config-file', type=str,
                        dest='config', help='configuration file')
    args = parser.parse_args()

    config_file = args.config
    if config_file is None:
        parser.error('no configuration file provided')
    if not os.path.exists(config_file):
        parser.error('configuration file %s does not exist' % config_file)

    config_parser = ConfigParser.ConfigParser()
    config_parser.read([str(config_file), ])

    serve(config_parser)
