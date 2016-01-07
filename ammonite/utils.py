import raven
import logging
import sys
import configparser
import argparse
import os
from unicodedata import normalize

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


def zipdir(path, zipf, root_folder):
    # Still need to find a clean way to write empty folders,
    # as this is not being done
    for root, _, files in os.walk(path):
        for fh in files:
            file_path = os.path.join(root, fh)
            arcname = os.path.relpath(file_path, root_folder)
            zip_file(file_path, zipf, arcname)


def zip_file(file_path, zipf, arcname):
    encodings = ["utf8", "ascii", "latin1"]
    try:
        zipf.write(file_path, arcname=arcname)
    except UnicodeEncodeError:
        narcname = os.fsencode(arcname)
        while encodings:
            try:
                narcname = str(narcname, encodings.pop(0))
                encodings = []
            except Exception:
                if not encodings:
                    narcname = normalize('NFKD',
                                         arcname).encode('ascii',
                                                         'ignore')
        zipf.write(file_path, arcname=narcname)
