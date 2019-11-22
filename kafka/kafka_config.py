import os

import yaml


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, 'config')


def _load_config(filename):
    with open(os.path.join(CONFIG_DIR, filename)) as config_file:
        return yaml.load(config_file) or {}


def load_producer_config():
    return {
        **_load_config('common.yaml'),
        **_load_config('producer.yaml')
    }


def load_consumer_config():
    return {
        **_load_config('common.yaml'),
        **_load_config('consumer.yaml')
    }
