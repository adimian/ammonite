import pika
import argparse
import ConfigParser
import os
import logging
import sys
import tempfile
import json
import docker
import requests
import zipfile
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
        if self.config.get('DOCKER', 'LOGIN'):
            client.login(self.config.get('DOCKER', 'LOGIN'),
                         self.config.get('DOCKER', 'PASSWORD'),
                         registry=self.config.get('DOCKER', 'REGISTRY'))
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
        return tempfile.mkdtemp(prefix='ammonite-%s-' % direction)

    def create_inbox(self):
        return self._create_temp_dir(direction='inbox')

    def create_outbox(self):
        return self._create_temp_dir(direction='outbox')

    def populate_inbox(self, inbox, execution_id, token):
        service = self.config.get('QUEUES', 'KABUTO_SERVICE')
        url = "%s/execution/%s/attachments/%s" % (service, execution_id, token)
        r = requests.get(url, stream=True)
        zip_dir = os.path.join(inbox, 'attachment.zip')
        with open(zip_dir, 'wb+') as fh:
            if not r.ok:
                raise Exception("Could not retrieve attachment")

            for block in r.iter_content(1024):
                if not block:
                    break
                fh.write(block)
        logger.info("extracting from %s" % zip_dir)
        with zipfile.ZipFile(zip_dir) as zf:
            logger.info("extracting in %s" % inbox)
            zf.extractall(inbox)
        os.remove(zip_dir)

    def upload_output(self, outbox, execution_id, token, data):
        zip_file = "%s.zip" % os.path.join(tempfile.mkdtemp(), token)
        zipf = zipfile.ZipFile(zip_file, 'w')
        zipdir(outbox, zipf, root_folder=outbox)
        zipf.close()

        service = self.config.get('QUEUES', 'KABUTO_SERVICE')
        url = "%s/execution/%s/results/%s" % (service, execution_id, token)
        files = [("results", open(zip_file, "rb"))]
        requests.post(url, files=files, data=data)

    def __call__(self, ch, method, properties, body):
        logger.info('received %r', body)
        logger.info('starting to work')

        recipe = json.loads(body)

        inbox = self.create_inbox()
        outbox = self.create_outbox()

        logger.info('downloading attachments')
        self.populate_inbox(inbox, recipe['execution'],
                            recipe['attachment_token'])
        logger.info('finished downloading attachments')

        docker_client = self.get_docker_client()
        image_name = '/'.join((self.config.get('DOCKER', 'REGISTRY_URL'),
                               recipe['image']))

        logger.info("creating container")
        # create_container fail if image is not pulled first:
        docker_client.pull(image_name, insecure_registry=True)
        container = docker_client.create_container(image=image_name,
                                                   command=recipe['command'],
                                                   volumes=[inbox, outbox])
        logger.info("finished creating container")

        logger.info('starting job')
        response = docker_client.start(container=container.get('Id'),
                                       binds={inbox: {'bind': '/inbox',
                                                      'ro': True},
                                       outbox: {'bind': '/outbox',
                                                'ro': False}})

        logs = docker_client.logs(container.get('Id'),
                                  stdout=True,
                                  stderr=True,
                                  stream=True,
                                  timestamps=True)
        for log in logs:
            logger.info(log)
            service = self.config.get('QUEUES', 'KABUTO_SERVICE')
            url = '%s/execution/%s/log/%s' % (service,
                                              recipe['execution'],
                                              recipe['result_token'])
            requests.post(url, data={"log_line": log})

        logger.info('finished job with response: %s' % response)

        logger.info('uploading results')
        data = {"state": "done",
                "response": response,
                "cpu": 0,
                "memory": 0,
                "io": 0}
        self.upload_output(outbox, recipe['execution'],
                           recipe['result_token'],
                           data)
        logger.info('finished uploading results')

        logger.info('done working')
        ch.basic_ack(delivery_tag=method.delivery_tag)


def zipdir(path, zipf, root_folder):
    # Still need to find a clean way to write empty folders,
    # as this is not being done
    for root, _, files in os.walk(path):
        for fh in files:
            file_path = os.path.join(root, fh)
            zipf.write(file_path, os.path.relpath(file_path, root_folder))


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
