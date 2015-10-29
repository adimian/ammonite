from ammonite.job_handler import ExecutionCallback
from ammonite.utils import setup_sentry, get_config
from ammonite.connection import Consumer


def main():
    config = get_config()
    setup_sentry(config)
    Consumer(config).consume(ExecutionCallback)


if __name__ == '__main__':
    main()
