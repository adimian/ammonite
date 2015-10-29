import raven
import logging
import sys
import configparser
import argparse
import os

logging.basicConfig(stream=sys.stdout,
                    format="%(asctime)s [%(levelname)s] %(message)s")

logger = logging.getLogger("ammonite.worker")
logger.level = logging.DEBUG

SENTRY_CLIENT = None


def setup_sentry(config):
    try:
        sentry_dsn = config.get('SENTRY', 'SENTRY_DSN')
    except configparser.NoSectionError:
        sentry_dsn = None

    if sentry_dsn:
        global SENTRY_CLIENT
        SENTRY_CLIENT = raven.Client(dsn=sentry_dsn)


def get_config():
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
