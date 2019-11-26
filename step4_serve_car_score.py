import os
import sys
import logging

from flask import Flask, jsonify

from tweets_loader import configure_logging
from kafka import KafkaConsumer, OFFSET_LATEST

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
NUMBER_OF_RETRIES = 10

configure_logging()

app = Flask(__name__)


def read_latest_score():
    consumer = KafkaConsumer(
        topic='car_score',
        value_schema_path=os.path.join(SCRIPT_DIR, 'kafka/schemas/car_score.avro'),
        manual_offset=OFFSET_LATEST,
        config={
            'enable.auto.commit': False,
            'auto.offset.reset': 'latest'
        }
    )
    with consumer:
        retries = NUMBER_OF_RETRIES
        while retries > 0:
            msg = consumer.poll(5)
            if msg is None:
                retries -= 1
                logging.info(f'Failed to read the latest score, retrying {retries} times')
                continue
            return msg.value()
        raise RuntimeError('Failed to read the latest score')


@app.route('/')
def serve_car_score():
    logging.info('Serving request')
    score = read_latest_score()
    logging.info(f'Score: {score}')
    response = jsonify(score)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


def main():
    try:

        print(read_latest_score())

    except Exception:
        logging.exception('Stopped worker')
        return 1


if __name__ == '__main__':
    sys.exit(main())
