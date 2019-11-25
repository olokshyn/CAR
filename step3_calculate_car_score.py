import os
import sys
import logging

from tweets_loader import configure_logging
from kafka import KafkaConsumer, KafkaProducer

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

configure_logging()


def main():
    try:
        consumer = KafkaConsumer(
            topic='sentiments',
            key_schema_path=os.path.join(SCRIPT_DIR, 'kafka/schemas/tweet_id.avro'),
            value_schema_path=os.path.join(SCRIPT_DIR, 'kafka/schemas/sentiment_value.avro')
        )

        producer = KafkaProducer(
            topic='car_score',
            value_schema_path=os.path.join(SCRIPT_DIR, 'kafka/schemas/car_score.avro')
        )

        first_tweet_id = None
        last_tweet_id = None
        score_sum = 0.0
        tweets_count = 0
        positive_tweets_count = 0
        neutral_tweets_count = 0
        negative_tweets_count = 0
        tweets_read_since_last_committed = 0

        logging.info('Started consuming messages')
        with consumer:
            while True:
                if tweets_read_since_last_committed < 100:
                    msg = consumer.poll(5)
                else:
                    msg = None
                    logging.debug('Have read too many tweets, dumping the score')
                if msg is None:
                    if tweets_count == 0:
                        logging.debug('Have not read a single tweet, continuing')
                        continue

                    assert first_tweet_id is not None
                    assert last_tweet_id is not None

                    if tweets_read_since_last_committed == 0:
                        logging.debug('Have not read any new tweets, continuing')
                        continue

                    score = score_sum / tweets_count
                    value = {
                        'score': score,
                        'tweets_count': tweets_count,
                        'positive_tweets_count': positive_tweets_count,
                        'neutral_tweets_count': neutral_tweets_count,
                        'negative_tweets_count': negative_tweets_count,
                        'first_tweet_id': first_tweet_id,
                        'last_tweet_id': last_tweet_id
                    }
                    logging.debug(f'Aggregated {tweets_count} tweets with score {score}, '
                                  f'[{first_tweet_id}, {last_tweet_id}]')
                    producer.send(value)
                    tweets_read_since_last_committed = 0

                else:
                    tweet_id = msg.key()
                    tweet_score = msg.value()
                    logging.debug(f'Got tweet {tweet_id} with score {tweet_score}')
                    if first_tweet_id is None:
                        first_tweet_id = tweet_id
                    last_tweet_id = tweet_id
                    score_sum += tweet_score
                    tweets_count += 1
                    if tweet_score > 0.0:
                        positive_tweets_count += 1
                    elif tweet_score == 0.0:
                        neutral_tweets_count += 1
                    else:
                        negative_tweets_count += 1
                    tweets_read_since_last_committed += 1

    except Exception:
        logging.exception('Stopped worker')
        return 1


if __name__ == '__main__':
    sys.exit(main())
