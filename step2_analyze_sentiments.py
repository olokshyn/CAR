import os
import sys
import logging
from functools import partial

from tweets_loader import configure_logging
from kafka import KafkaConsumer, KafkaProducer
from tweets_analyzer import TweetsAnalyzer
from textblob.download_corpora import download_all

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

configure_logging()


def analyze_tweet(tweet_id, tweet, analyzer: TweetsAnalyzer, producer: KafkaProducer):
    logging.debug(f'Got message: {tweet_id}: {tweet["full_text"]}')
    score = analyzer.analyze(tweet['full_text'])
    logging.debug(f'Analyzed message {tweet_id}, got score: {score}')
    producer.send(key=tweet_id, value=score)


def main():
    try:
        download_all()

        analyzer = TweetsAnalyzer()
        consumer = KafkaConsumer(
            topic='tweets',
            key_schema_path=os.path.join(SCRIPT_DIR, 'kafka/schemas/tweet_id.avro'),
            value_schema_path=os.path.join(SCRIPT_DIR, 'kafka/schemas/tweet_value.avro')
        )
        producer = KafkaProducer(
            topic='sentiments',
            key_schema_path=os.path.join(SCRIPT_DIR, 'kafka/schemas/tweet_id.avro'),
            value_schema_path=os.path.join(SCRIPT_DIR, 'kafka/schemas/sentiment_value.avro')
        )

        logging.info('Started consuming messages')
        consumer.process_messages(
            partial(analyze_tweet, analyzer=analyzer, producer=producer)
        )

    except Exception:
        logging.exception('Stopped worker')
        return 1


if __name__ == '__main__':
    sys.exit(main())
