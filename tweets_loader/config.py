import os

import yaml
from logging.config import dictConfig


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, 'config')


def configure_logging():
    with open(os.path.join(CONFIG_DIR, 'logging_config.yaml')) as logging_config_file:
        logging_config = yaml.load(logging_config_file, Loader=yaml.SafeLoader)

    dictConfig(logging_config)
