import argparse
import configparser
import json
import logging
import os
import sys
import tempfile
import zipfile
import time
import uuid

import docker
import pika
import requests
import raven

logging.basicConfig(stream=sys.stdout,
                    format="%(asctime)s [%(levelname)s] %(message)s")

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

SENTRY_CLIENT = None


class ExecutionCallback(object):

    def __init__(self, connection, config):
        self.connection = connection
        self.config = config
        self.log_buffer = []
        self.last_sent_log_time = time.time()
        self.root_dir = os.environ.get('AMMONITE_BOXES_DIR', None)
        if not self.root_dir:
            logger.info("Environment variable AMMONITE_BOXES_DIR not set")

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
        if self.root_dir:
            dir_name = 'ammonite-%s-%s' % (direction, uuid.uuid4())
            dir_path = os.path.join(self.root_dir, dir_name)
            os.mkdir(dir_path)
        else:
            dir_path = tempfile.mkdtemp(prefix='ammonite-%s-' % direction)
        logger.info('Creating %s dir %s' % (direction, dir_path))
        return dir_path

    def create_inbox(self):
        return self._create_temp_dir(direction='inbox')

    def create_outbox(self):
        return self._create_temp_dir(direction='outbox')

    def populate_inbox(self, inbox, execution_id, token):
        service = self.config.get('QUEUES', 'KABUTO_SERVICE')
        url = "%s/execution/%s/attachments/%s" % (service, execution_id, token)
        r = requests.get(url, stream=True)
        if not r.ok:
            raise Exception("Could not retrieve attachment")
        zip_dir = os.path.join(inbox, 'attachment.zip')
        with open(zip_dir, 'wb+') as fh:

            for block in r.iter_content(1024):
                if not block:
                    break
                fh.write(block)
        with zipfile.ZipFile(zip_dir) as zf:
            logger.info("extracting in %s" % inbox)
            zf.extractall(inbox)
        os.remove(zip_dir)

    def prepare_output(self, outbox, token):
        zip_file = "%s.zip" % os.path.join(tempfile.mkdtemp(), token)
        zipf = zipfile.ZipFile(zip_file, 'w')
        zipdir(outbox, zipf, root_folder=outbox)
        zipf.close()
        logger.info("creating zip from %s" % outbox)
        files = [("results", open(zip_file, "rb"))]
        return files

    def upload_output(self, execution_id, token, data, files):
        service = self.config.get('QUEUES', 'KABUTO_SERVICE')
        url = "%s/execution/%s/results/%s" % (service, execution_id, token)
        return requests.post(url, files=files, data=data)

    def notify_error(self, recipe, log_url):
        message = "An unexpected error occured. Please contact your admin"
        self.send_logs(message, log_url, force=True)
        data = {"state": 'failed',
                "response":-1,
                "cpu": 0,
                "memory": 0,
                "io": 0}
        self.upload_output(recipe['execution'],
                           recipe['result_token'],
                           data, [])

    def __call__(self, ch, method, properties, body):
        logger.info('received %r', body)
        recipe = json.loads(body.decode('utf-8'))
        service = self.config.get('QUEUES', 'KABUTO_SERVICE')
        log_url = '%s/execution/%s/log/%s' % (service,
                                              recipe['execution'],
                                              recipe['result_token'])
        try:
            self._call(ch, method, properties, recipe, log_url)
        except Exception as e:
            print(e)
            # notify error to kabuto
            self.notify_error(recipe, log_url)
            # skip job
            ch.basic_ack(delivery_tag=method.delivery_tag)
            if SENTRY_CLIENT:
                SENTRY_CLIENT.captureException()

    def _call(self, ch, method, properties, recipe, log_url):
        logger.info('starting to work')

        inbox = self.create_inbox()
        outbox = self.create_outbox()

        logger.info('downloading attachments')
        self.populate_inbox(inbox, recipe['execution'],
                            recipe['attachment_token'])
        logger.info('finished downloading attachments')

        docker_client = self.get_docker_client()
        image_name = recipe['image_tag']

        logger.info("creating container")
        # create_container fail if image is not pulled first:
        docker_client.pull(image_name, insecure_registry=True)
        mem_limit = self.config.get('DOCKER', 'MEMORY_LIMIT')

        state = "done"
        response = -1

        try:
            container = docker_client.create_container(image=image_name,
                                                       command=recipe['command'],
                                                       volumes=[inbox, outbox],
                                                       mem_limit=mem_limit)
            logger.info("finished creating container")
            logger.info('starting job')
            docker_client.start(container=container.get('Id'),
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
                self.send_logs(log, log_url)

            response = docker_client.wait(container=container.get('Id'))
            logger.info('finished job with response: %s' % response)
        except docker.errors.APIError as e:
            state = 'failed'
            logger.critical("Docker API error: %s" % e)
            self.send_logs(e, log_url)
        except Exception as e:
            state = "failed"
            logger.critical("Exception: %s" % e)
            self.send_logs(e, log_url)

        logger.info('uploading results')
        data = {"state": state,
                "response": response,
                "cpu": 0,
                "memory": 0,
                "io": 0}

        try:
            files = self.prepare_output(outbox, recipe['result_token'])
        except UnicodeEncodeError:
            data["state"] = 'failed'
            message = ("A character could not be decoded in an output "
                       "filename. Make sure your filenames are OS friendly")
            self.send_logs(message, log_url)
            files = []
        except Exception as e:
            data["state"] = 'failed'
            message = ("Something unexpected happened: %s" % e)
            self.send_logs(message, log_url)
            files = []
        self.upload_output(recipe['execution'],
                           recipe['result_token'],
                           data, files)

        self.send_logs("", log_url, force=True)
        logger.info('finished uploading results')

        logger.info('done working')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_logs(self, log, url, force=False):
        logger.info(log)
        # forcing if last sending was longer than a second
        send_log_time = time.time()
        if not force:
            force = (send_log_time - self.last_sent_log_time) > 1
        # prevent from flooding the kabuto http server by
        # sending sporadic updates instead of each line separate
        self.log_buffer.append(str(log))
        if len(self.log_buffer) >= 20 or force:
            logs = json.dumps(self.log_buffer)
            logger.info('Sending logs')
            requests.post(url, data={"log_line": logs})
            self.last_sent_log_time = send_log_time
            self.log_buffer = []


def zipdir(path, zipf, root_folder):
    # Still need to find a clean way to write empty folders,
    # as this is not being done
    for root, _, files in os.walk(path):
        for fh in files:
            file_path = os.path.join(root, fh)
            zipf.write(file_path, os.path.relpath(file_path, root_folder))


def serve(config):
    connection = get_connection(config)
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


def get_connection(config):
    credentials = pika.PlainCredentials(
        config.get('AMQP', 'USER'),
        config.get('AMQP', 'PASSWORD'),
    )
    parameters = pika.ConnectionParameters(
        host=config.get('AMQP', 'HOSTNAME'),
        credentials=credentials,
    )

    def connect(timeout):
        try:
            connection = pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError as error:
            logger.error("Could not connect. Retrying in %ss" % timeout)
            time.sleep(timeout)
            return False, error
        return True, connection

    timeout = 1
    while timeout <= 8:
        connected, connection = connect(timeout)
        if connected:
            break
        else:
            timeout = timeout * 2

    if not connected:
        raise connection
    return connection


def prepare_config():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--config-file', type=str,
                        dest='config', help='configuration file')
    args = parser.parse_args()

    config_file = args.config or os.environ.get('AMMONITE_CONFIG', '')
    if not config_file:
        parser.error('no configuration file provided')
    if not os.path.exists(config_file):
        parser.error('configuration file %s does not exist' % config_file)

    config_parser = configparser.ConfigParser()
    config_parser.read([str(config_file), ])
    return config_parser


def main():
    config = prepare_config()
    try:
        sentry_dsn = config.get('SENTRY', 'SENTRY_DSN')
    except configparser.NoSectionError:
        sentry_dsn = None

    if sentry_dsn:
        global SENTRY_CLIENT
        SENTRY_CLIENT = raven.Client(dsn=sentry_dsn)
        try:
            serve(config)
        except Exception:
            SENTRY_CLIENT.captureException()
    else:
        serve(config)


if __name__ == '__main__':
    main()
