from ammonite.callback import ExecutionCallback, KillCallback
from ammonite.utils import setup_sentry, get_config, logger
from ammonite.connection import Consumer
from threading import Thread


def main():
    config = get_config()
    setup_sentry(config)

    logger.info('---')
    logger.info('--- Ammonite worker ready.')
    logger.info('--- Waiting for messages.')
    logger.info('--- To exit press CTRL+C')
    logger.info('---')

    Thread(target=Consumer(config).consume,
           args=(ExecutionCallback, config.get('QUEUES', 'JOBS'))).start()
    Thread(target=Consumer(config).consume,
           args=(KillCallback, config.get('QUEUES', 'KILL'), True)).start()


if __name__ == '__main__':
    main()
