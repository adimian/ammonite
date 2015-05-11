from worker import (serve, prepare_config, main, ExecutionCallback,
                    get_connection)
from unittest.mock import patch
import pytest
import os
import shutil
import json

ROOT_DIR = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
CONF_PATH = os.path.join(ROOT_DIR, "data", "test_config.cfg")

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
        print("adding")
        global MOCK_POST_REQUEST
        MOCK_POST_REQUEST.append(self)
        self.url = url
        self.files = files
        self.data = data


@patch('pika.PlainCredentials')
@patch('pika.ConnectionParameters')
@patch('pika.BlockingConnection')
@pytest.fixture
def execution(pc_mock, cp_mock, bc_mock):
    testargs = ["ammonite.py", "-f", CONF_PATH]
    with patch('sys.argv', testargs):
        config = prepare_config()
        return ExecutionCallback(get_connection(config), config)


def test_config():
    testargs = ["ammonite.py"]
    with patch('sys.argv', testargs):
        pytest.raises(SystemExit, prepare_config)

    testargs = ["ammonite.py", "-f", "does/not/exist"]
    with patch('sys.argv', testargs):
        pytest.raises(SystemExit, prepare_config)

    testargs = ["ammonite.py", "-f", CONF_PATH]
    with patch('sys.argv', testargs):
        config = prepare_config()
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


def test_put_in_message_queue(execution):
    execution.put_in_message_queue(None, None)


def test_populate_inbox(execution):
    with patch('requests.get', mockGetRequest):
        inbox = execution.create_inbox()
        execution.populate_inbox(inbox, "1", "some_token")
        files = os.listdir(inbox)
        assert len(files) == 2
        assert sorted(["file1.txt", "file2.txt"]) == sorted(files)
        pytest.raises(Exception, execution.populate_inbox, inbox, "1", "false_token")
        shutil.rmtree(inbox)


def test_upload_output(execution):
    with patch('requests.post', mockPostRequest):
        outbox = os.path.join(ROOT_DIR, "data", "outbox")
        data = {"state": "done",
                "response": 0,
                "cpu": 0,
                "memory": 0,
                "io": 0}
        r = execution.upload_output(outbox, "1", "some_token", data)
        files = dict(r.files)
        assert files.get('results')
        assert "some_token.zip" in files['results'].name
        assert r.data == data
        shutil.rmtree(os.path.dirname(files['results'].name))


@patch('requests.post', mockPostRequest)
def test_send_logs(execution):
    global MOCK_GET_REQUEST
    global MOCK_POST_REQUEST
    MOCK_GET_REQUEST = []
    MOCK_POST_REQUEST = []
    log_line = "a log line"
    execution.send_logs(log_line, "some_url", force=False)
    assert execution.log_buffer == [log_line]
    execution.send_logs(log_line, "some_url", force=True)
    assert execution.log_buffer == []
    assert len(MOCK_POST_REQUEST) == 1
    assert MOCK_POST_REQUEST[0].data == {"log_line": [log_line, log_line]}


@patch('requests.get', mockGetRequest)
@patch('requests.post', mockPostRequest)
@patch('docker.Client', mockClient)
def test_call(execution):
    global MOCK_GET_REQUEST
    global MOCK_POST_REQUEST
    MOCK_GET_REQUEST = []
    MOCK_POST_REQUEST = []

    class mockChannel(object):
        def basic_ack(self, *args, **kwargs):
            pass

    class mockMethod(object):
        delivery_tag = None

    body = {'execution': 1,
            'image': "some_image",
            'command': "some_command",
            'attachment_token': "some_token",
            'result_token': "some_result_token"}
    execution(mockChannel(), mockMethod(), None,
              json.dumps(body).encode(encoding='utf-8'))
    assert len(MOCK_GET_REQUEST) == 1
    assert len(MOCK_POST_REQUEST) == 2

    expected_log = {'log_line': ['log line one', 'log line two', '']}
    expected_data = {'io': 0, 'cpu': 0, 'state': 'done',
                     'memory': 0, 'response': 0}
    assert MOCK_POST_REQUEST[0].data == expected_log
    assert MOCK_POST_REQUEST[1].data == expected_data

    MOCK_GET_REQUEST = []
    MOCK_POST_REQUEST = []
    body['image'] = "throw_error"
    execution(mockChannel(), mockMethod(), None,
              json.dumps(body).encode(encoding='utf-8'))
    expected_data = {'io': 0, 'cpu': 0, 'state': 'failed',
                     'memory': 0, 'response': 1}
    assert MOCK_POST_REQUEST[1].data == expected_data
