from ammonite.worker import main
from ammonite.callback import ExecutionCallback, KillCallback
from ammonite.utils import get_config, zipdir
from unittest.mock import patch
import pytest
import os
import shutil
import json
import tempfile
from testfixtures import LogCapture
import zipfile

ROOT_DIR = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
CONF_PATH = os.path.join(DATA_DIR, "test_config.cfg")

MOCK_GET_REQUEST = []
MOCK_POST_REQUEST = []

MOCK_LOG_LINES = ["log line one", "log line two"]


class mockClient(object):
    def __init__(self, base_url):
        self.base_url = base_url
        self.throw_error = False

    def login(self, login, password, registry):
        self.login = login
        self.password = password
        self.registry = registry

    def pull(self, *args, **kwargs):
        pass

    def create_container(self, *args, **kwargs):
        image = kwargs.get('image', "")
        if "throw_error" in image:
            self.throw_error = True
        return {'Id': 1}

    def start(self, *args, **kwargs):
        # we want to throw an error in one test
        # to test the exception handling
        if self.throw_error:
            raise Exception("run failed")

    def logs(self, *args, **kwargs):
        return MOCK_LOG_LINES

    def wait(self, *args, **kwargs):
        if self.throw_error:
            return 1
        return 0

    def containers(self, *args, **kwargs):
        return [{"Id": 1}]

    def kill(self, *args, **kwargs):
        return None


class mockGetRequest(object):
    def __init__(self, url, stream):
        print("adding")
        global MOCK_GET_REQUEST
        MOCK_GET_REQUEST.append(self)
        self.url = url
        self.stream = stream

    @property
    def ok(self):
        if 'some_token' in self.url:
            return True
        return False

    def iter_content(self, block):
        zipf = os.path.join(ROOT_DIR, "data", "some_test.zip")
        fh = open(zipf, 'rb')
        while True:
            data = fh.read(block)
            yield data


class mockPostRequest(object):
    def __init__(self, url, files=None, data=None):
        global MOCK_POST_REQUEST
        MOCK_POST_REQUEST.append(self)
        self.url = url
        self.files = files
        self.data = data


class mockSender(object):
    def __init__(self):
        self.messages = []
        self.broadcast = []

    def connect(self):
        pass

    def get_connection(self):
        pass

    def open_channel(self):
        pass

    def send(self, message, queue_name=None):
        self.messages.append(message)

    def broadcast(self, message, exchange_name=None):
        self.broadcast.append(message)


@patch('pika.PlainCredentials')
@patch('pika.ConnectionParameters')
@patch('pika.BlockingConnection')
@pytest.fixture
def execution(pc_mock, cp_mock, bc_mock):
    testargs = ["ammonite.py", "-f", CONF_PATH]
    with patch('sys.argv', testargs):
        config = get_config()
    execution = ExecutionCallback(config)
    execution.sender = mockSender()
    return execution


@patch('pika.PlainCredentials')
@patch('pika.ConnectionParameters')
@patch('pika.BlockingConnection')
@pytest.fixture
def kill_execution(pc_mock, cp_mock, bc_mock):
    testargs = ["ammonite.py", "-f", CONF_PATH]
    with patch('sys.argv', testargs):
        config = get_config()
        return KillCallback(config)


def test_config():
    testargs = ["ammonite.py"]
    with patch('sys.argv', testargs):
        pytest.raises(SystemExit, get_config)

    testargs = ["ammonite.py", "-f", "does/not/exist"]
    with patch('sys.argv', testargs):
        pytest.raises(SystemExit, get_config)

    testargs = ["ammonite.py", "-f", CONF_PATH]
    with patch('sys.argv', testargs):
        config = get_config()
        assert config.get('QUEUES', 'JOBS') == "jobs"
        assert config.get('AMQP', 'USER') == "ammonite"
        assert config.get('WORKER', 'SLOTS') == '2'
        assert config.get('DOCKER', 'LOGIN') == "adimian"


@patch('pika.PlainCredentials')
@patch('pika.ConnectionParameters')
@patch('pika.BlockingConnection')
def test_main(pc_mock, cp_mock, bc_mock):
    testargs = ["ammonite.py", "-f", CONF_PATH]
    with patch('sys.argv', testargs):
        main()


def test_get_docker_client(execution):
    with patch('docker.Client', mockClient):
        client = execution.get_docker_client()
        assert client.login == "adimian"
        assert client.password == "adimian"


def test_populate_inbox(execution):
    with patch('requests.get', mockGetRequest):
        inbox = execution.create_inbox()
        execution.populate_inbox(inbox, "1", "some_token", "some_container_id")
        files = os.listdir(inbox)
        assert len(files) == 2
        assert sorted(["file1.txt", "file2.txt"]) == sorted(files)
        pytest.raises(Exception,
                      execution.populate_inbox, inbox,
                      "1", "false_token")
        shutil.rmtree(inbox)


def test_upload_output(execution):
    with patch('requests.post', mockPostRequest):
        outbox = os.path.join(ROOT_DIR, "data", "outbox")
        data = {"state": "done",
                "response": 0,
                "cpu": 0,
                "memory": 0,
                "io": 0}
        files = execution.prepare_output(outbox, "some_token")
        r = execution.upload_output("1", "some_token", data, files)
        files = dict(r.files)
        assert files.get('results')
        assert "some_token.zip" in files['results'].name
        assert r.data == data
        shutil.rmtree(os.path.dirname(files['results'].name))


@patch('requests.post', mockPostRequest)
def test_send_logs(execution):
    log_line = "a log line"
    execution.send_logs(log_line, force=False)
    execution.recipe = {"execution": 1}
    assert execution.log_buffer == [log_line]
    execution.send_logs(log_line, force=True)
    assert execution.log_buffer == []

    expected = [{'log_lines': ['a log line', 'a log line'], 'job_id': 1}]
    assert expected == execution.sender.messages


class mockChannel(object):
    def basic_ack(self, *args, **kwargs):
        pass


class mockMethod(object):
    delivery_tag = None


@patch('requests.get', mockGetRequest)
@patch('requests.post', mockPostRequest)
@patch('docker.Client', mockClient)
def test_call(execution):
    global MOCK_GET_REQUEST
    global MOCK_POST_REQUEST
    MOCK_GET_REQUEST = []
    MOCK_POST_REQUEST = []

    body = {'execution': 1,
            'image': "some_image",
            'command': "some_command",
            'attachment_token': "some_token",
            'result_token': "some_result_token",
            'image_tag': "some_tag"}
    execution(mockChannel(), mockMethod(), None,
              json.dumps(body).encode(encoding='utf-8'))
    assert len(MOCK_GET_REQUEST) == 1
    assert len(MOCK_POST_REQUEST) == 1

    expected_log = {'log_line': json.dumps(['log line one',
                                            'log line two',
                                            ''])}
    expected_data = {'io': 0, 'cpu': 0, 'state': 'done',
                     'memory': 0, 'response': 0}

    assert MOCK_POST_REQUEST[0].data == expected_data

    MOCK_GET_REQUEST = []
    MOCK_POST_REQUEST = []
    body['image_tag'] = "throw_error"
    execution(mockChannel(), mockMethod(), None,
              json.dumps(body).encode(encoding='utf-8'))
    expected_data = {'io': 0, 'cpu': 0, 'state': 'failed',
                     'memory': 0, 'response':-1}
    print(MOCK_POST_REQUEST[0].data)
    assert MOCK_POST_REQUEST[0].data == expected_data


@patch('pika.PlainCredentials')
@patch('pika.ConnectionParameters')
@patch('pika.BlockingConnection')
def test_create_temp_dir(pc_mock, cp_mock, bc_mock):
    testargs = ["ammonite.py", "-f", CONF_PATH]
    with patch('sys.argv', testargs):
        config = get_config()
        execution = ExecutionCallback(config)
    path = execution._create_temp_dir("inbox")
    temp_path = tempfile.mkdtemp(prefix='ammonite-')
    assert path.startswith("%s/ammonite-inbox" % os.path.dirname(temp_path))

    testargs = ["ammonite.py", "-f", CONF_PATH]
    ammonite_path = os.path.join(ROOT_DIR, "data")
    os.environ["AMMONITE_BOXES_DIR"] = ammonite_path
    with patch('sys.argv', testargs):
        config = get_config()
        execution = ExecutionCallback(config)
    path = execution._create_temp_dir("inbox")
    assert path.startswith(ammonite_path)
    shutil.rmtree(temp_path)
    shutil.rmtree(path)


@patch('requests.get', mockGetRequest)
@patch('requests.post', mockPostRequest)
@patch('docker.Client', mockClient)
def test_unicode_error(execution):
    def prepare_output(*args, **kwargs):
        raise UnicodeEncodeError('hitchhiker', "", 42, 43,
                                 'the universe and everything else')
    with patch("worker.ExecutionCallback.prepare_output", prepare_output):
        body = {'execution': 1,
                'image': "some_image",
                'command': "some_command",
                'attachment_token': "some_token",
                'result_token': "some_result_token",
                'image_tag': "some_tag"}
        expected = ('ammonite.worker', 'INFO',
                    ('A character could not be decoded in an output filename.'
                     ' Make sure your filenames are OS friendly'))
        with LogCapture() as l:
            execution(mockChannel(), mockMethod(), None,
                      json.dumps(body).encode(encoding='utf-8'))
            assert expected in tuple(l.actual())


@patch('requests.get', mockGetRequest)
@patch('requests.post', mockPostRequest)
@patch('docker.Client', mockClient)
def test_kill_job(kill_execution):
    body = {'container_id': 1}
    with LogCapture() as l:
        kill_execution(mockChannel(), mockMethod(), None,
                       json.dumps(body).encode(encoding='utf-8'))
        expected = (("ammonite.worker", "INFO",
                     "received kill signal for container '1'"),
                    ("ammonite.worker", "INFO",
                     "requesting local container ids"),
                    ("ammonite.worker", "INFO",
                     "container killed 1"))
        l.check(*expected)


@patch('requests.get', mockGetRequest)
@patch('requests.post', mockPostRequest)
@patch('docker.Client', mockClient)
def test_kill_job_non_existing_container(kill_execution):
    body = {'container_id': 2}
    with LogCapture() as l:
        kill_execution(mockChannel(), mockMethod(), None,
                       json.dumps(body).encode(encoding='utf-8'))
        expected = (("ammonite.worker", "INFO",
                     "received kill signal for container '2'"),
                    ("ammonite.worker", "INFO",
                     "requesting local container ids"),
                    ("ammonite.worker", "INFO",
                     "container not on this machine"))
        l.check(*expected)


@patch('requests.get', mockGetRequest)
@patch('requests.post', mockPostRequest)
@patch('docker.Client', mockClient)
def test_kill_job_general_error(kill_execution):
    body = {'container_id': 1}

    def kill_container(*args, **kwargs):
        raise Exception("some error")

    with patch("worker.KillCallback.call", kill_container):
        with LogCapture() as l:
            kill_execution(mockChannel(), mockMethod(), None,
                           json.dumps(body).encode(encoding='utf-8'))
            expected = ("ammonite.connection", "CRITICAL",
                        'Exception: some error')
            l.check(expected)


@patch('pika.PlainCredentials')
@patch('pika.ConnectionParameters')
@patch('pika.BlockingConnection')
def test_sender(a, b, c):
    from ammonite.connection import Sender
    sender = Sender("some_queue", {"AMQP_HOSTNAME": "",
                                   "AMQP_USER": "",
                                   "AMQP_PASSWORD": ""})
    sender.send(['some message'])
    sender.broadcast(['some message'])


WRITE_FUNC = zipfile.ZipFile.write


def unicode_error(*args, **kwargs):
    for c in kwargs['arcname']:
        if ord(c) > 128:
            raise UnicodeEncodeError('hitchhiker', "", 42, 43,
                                     'the universe and everything else')
    zipf = args[0]
    zipf.write = WRITE_FUNC
    zipf.write(*args, **kwargs)


@patch('zipfile.ZipFile.write', unicode_error)
def test_zip_file():
    dir_path = os.path.join(DATA_DIR, "non-ascii")
    zip_file = "%s.zip" % os.path.join(tempfile.mkdtemp(), "test")
    zipf = zipfile.ZipFile(zip_file, 'w')
    zipdir(dir_path, zipf, root_folder=dir_path)
    zipf.close()

    expected = 'non-ascii-e769.txt'
    with zipfile.ZipFile(zip_file) as zf:
        assert expected in [i.filename for i in zf.infolist()]
