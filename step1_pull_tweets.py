import os
import sys
import logging

from tweets_loader import TweetsLoader, configure_logging
from kafka import KafkaProducer

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PAGES_PER_REQUEST = 5

configure_logging()


def main():
    try:

        loader = TweetsLoader(q='brexit')
        producer = KafkaProducer(
            topic='tweets',
            key_schema_path=os.path.join(SCRIPT_DIR, 'kafka/schemas/tweet_id.avro'),
            value_schema_path=os.path.join(SCRIPT_DIR, 'kafka/schemas/tweet_value.avro')
        )
        for tweet in loader.stream_tweets(PAGES_PER_REQUEST):
            tweet_value = {
                'created_at': tweet['created_at'],
                'full_text': tweet['full_text']
            }
            logging.debug(f'Got tweet {tweet["id"]}: {tweet["full_text"]}')
            producer.send(key=tweet['id'], value=tweet_value)
            logging.debug(f'Produced tweet {tweet["id"]}')

    except Exception:
        logging.exception('Stopped worker')
        return 1


if __name__ == '__main__':
    sys.exit(main())
