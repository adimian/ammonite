from ammonite.callback import handle_job, KillCallback
from ammonite.utils import setup_sentry, get_config, logger
from ammonite.connection import Receiver
from threading import Thread


def main():
    config = get_config()
    setup_sentry(config)

    logger.info('---')
    logger.info('--- Ammonite worker ready.')
    logger.info('--- Waiting for messages.')
    logger.info('--- To exit press CTRL+C')
    logger.info('---')

    job_listener = Receiver(config.get('QUEUES', 'JOBS'), config)
    kill_listener = Receiver(config.get('QUEUES', 'KILL'), config)
    job_listener.threaded_listen(handle_job)
    kill_listener.threaded_listen(KillCallback(config), True)


if __name__ == '__main__':
    main()
