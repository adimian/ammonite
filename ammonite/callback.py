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
from ammonite.utils import SENTRY_CLIENT, zipdir
from ammonite.connection import Sender, BaseHandler
from threading import Thread

logging.basicConfig(stream=sys.stdout,
                    format="%(asctime)s [%(levelname)s] %(message)s")

logger = logging.getLogger("ammonite.worker")
logger.level = logging.DEBUG


class Base(BaseHandler):
    def get_docker_client(self):
        client = docker.Client(base_url=self.config.get('DOCKER', 'ENDPOINT'))
        if self.config.get('DOCKER', 'LOGIN'):
            client.login(self.config.get('DOCKER', 'LOGIN'),
                         self.config.get('DOCKER', 'PASSWORD'),
                         registry=self.config.get('DOCKER', 'REGISTRY'))
        return client


class KillCallback(Base):
    def __init__(self, config):
        self.config = config

    def call(self, recipe):
        cid = recipe['container_id']
        logger.info("received kill signal for container '%s'" % cid)
        logger.info("requesting local container ids")
        docker_client = self.get_docker_client()
        if cid in [cont['Id'] for cont in docker_client.containers()]:
            docker_client.kill(cid)
            logger.info("container killed %s" % cid)
        else:
            logger.info("container not on this machine")


def handle_job(ch, method, properties, body):
    def execution_callback(ch, method, properties, body):
        ExecutionCallback(ch.receiver.config)(ch, method, properties, body)
    logger.info("Starting job thread")
    thread = Thread(
        target=execution_callback,
        args=(ch, method, properties, body)
    )
    thread.start()
    logger.info("thread started")


class ExecutionCallback(Base):
    def __init__(self, config):
        self.config = config
        self.log_buffer = []
        self.last_sent_log_time = time.time()
        self.root_dir = os.environ.get('AMMONITE_BOXES_DIR', None)
        self.container_id = None
        if not self.root_dir:
            logger.info("Environment variable AMMONITE_BOXES_DIR not set")

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

    def populate_inbox(self, inbox, execution_id, token, container_id):
        service = self.config.get('QUEUES', 'KABUTO_SERVICE')
        url = "%s/execution/%s/attachments/%s/%s" % (service, execution_id,
                                                     token, container_id)
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

    def notify_error(self, recipe):
        message = "An unexpected error occured. Please contact your admin"
        data = {"state": 'failed',
                "response":-1,
                "cpu": 0,
                "memory": 0,
                "io": 0,
                "errors": [message]}
        self.upload_output(recipe['execution'],
                           recipe['result_token'],
                           data, [])

    def __call__(self, ch, method, properties, body):
        try:
            logger.info('received %r', body)
            recipe = json.loads(body.decode('utf-8'))
            self.recipe = recipe
            self._call(method, properties, recipe)
        except Exception as e:
            print(e)
            # notify error to kabuto
            try:
                self.notify_error(recipe)
            except Exception as e:
                print(e)
                logger.info('Could not connect to the kabuto service')
            # skip job
            logger.info('Skipped job due to error')
            if SENTRY_CLIENT:
                SENTRY_CLIENT.captureException()
        logger.info("sending ack")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _call(self, method, properties, recipe):
        logger.info('starting to work for execution: %s' % recipe['execution'])
        errors = []

        inbox = self.create_inbox()
        outbox = self.create_outbox()

        docker_client = self.get_docker_client()
        image_name = recipe['image_tag']

        logger.info("creating container")
        # create_container fail if image is not pulled first:
        docker_client.pull(image_name, insecure_registry=True)

        state = "done"
        response = -1

        try:
            container = docker_client.create_container(image=image_name,
                                                       command=recipe['command'],
                                                       volumes=[inbox, outbox])
            cid = container.get('Id')
            logger.info("finished creating container: %s" % cid)

            logger.info('downloading attachments')
            self.populate_inbox(inbox, recipe['execution'],
                                recipe['attachment_token'],
                                container.get('Id'))
            logger.info('finished downloading attachments')

            logger.info('starting job')
            docker_client.start(container=cid,
                                binds={inbox: {'bind': '/inbox',
                                               'ro': True},
                                       outbox: {'bind': '/outbox',
                                                'ro': False}})

            thread = Thread(target=stream_log, args=(cid,
                                                     recipe['execution'],
                                                     docker_client,
                                                     self.config))
            thread.daemon = True
            thread.start()

            response = docker_client.wait(container=container.get('Id'))
            thread.join()
            logger.info('finished job with response: %s' % response)
        except docker.errors.APIError as e:
            state = 'failed'
            run_message = "Docker API error: %s" % e
            logger.critical(run_message)
            errors.append(run_message)
        except Exception as e:
            state = "failed"
            run_message = "Exception: %s" % e
            logger.critical(run_message)
            errors.append(run_message)

        logger.info('uploading results')
        data = {"state": state,
                "response": response,
                "cpu": 0,
                "memory": 0,
                "io": 0}

        try:
            files = self.prepare_output(outbox, recipe['result_token'])
        except UnicodeEncodeError as e:
            data["state"] = 'failed'
            message = ("A character could not be decoded in an output "
                       "filename. Make sure your filenames are OS friendly")
            errors.append(message)
            files = []
        except Exception as e:
            data["state"] = 'failed'
            message = ("Something unexpected happened: %s" % e)
            errors.append(message)
            files = []

        data['errors'] = errors
        self.upload_output(recipe['execution'],
                           recipe['result_token'],
                           data, files)

        logger.info('finished uploading results')
        logger.info('done working')


def stream_log(cid, jid, docker_client, config):
    logger.info('started sending logs for container: %s' % cid)
    container_path = config.get('DOCKER', 'CONTAINER_PATH')
    sender = Sender(broadcast=False,
                    queue_name='logs',
                    config=config)
    stream = True
    path = "{0}/{1}/{1}-json.log".format(container_path, cid)
    logger.info('Starting the streaming')
    retries = 5
    exists = False
    for retry in range(retries):
        if retry > 0:
            logger.info('Retrying to stream logs, attempt: %s' % retry)
        if os.path.exists(path):
            exists = True
            break
        time.sleep(2)
    if not exists:
        logger.info("can't stream logs, %s does not exist" % path)
    with open(path, "r+") as fh:
        while stream:
            state = docker_client.inspect_container(cid)['State']['Running']
            logs = fh.read(1000000)
            if logs:
                sender.send({"job_id": jid,
                             "log_lines": logs})
            elif not state:
                stream = False
    logger.info('finished sending logs for container: %s' % cid)
