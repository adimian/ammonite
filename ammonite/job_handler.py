import json
import logging
import os
import sys
import tempfile
import zipfile
import time
import uuid

import docker
import requests
from ammonite.utils import SENTRY_CLIENT

logging.basicConfig(stream=sys.stdout,
                    format="%(asctime)s [%(levelname)s] %(message)s")

logger = logging.getLogger("ammonite.worker")
logger.level = logging.DEBUG


class ExecutionCallback(object):

    def __init__(self, config):
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
        try:
            recipe = json.loads(body.decode('utf-8'))
            service = self.config.get('QUEUES', 'KABUTO_SERVICE')
            log_url = '%s/execution/%s/log/%s' % (service,
                                                  recipe['execution'],
                                                  recipe['result_token'])
            self._call(ch, method, properties, recipe, log_url)
        except Exception as e:
            print(e)
            # notify error to kabuto
            try:
                self.notify_error(recipe, log_url)
            except Exception:
                logger.info('Could not connect to the kabuto service')
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
