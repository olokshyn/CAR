import os
import itertools
import logging
import time

import yaml
from twitter import (
    Twitter,
    OAuth2, oauth2_dance,
    TwitterHTTPError
)


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
SECRETS_DIR = os.path.join(SCRIPT_DIR, 'secrets')
BEARER_TOKEN_FILENAME = os.path.join(SECRETS_DIR, 'bearer_token')


def get_app_creds():
    """
    Load consumer key and consumer secret from secret.yml

    secret.yml should have the following two properties:
    - consumer_key
    - consumer_secret
    """
    with open(os.path.join(SECRETS_DIR, 'secret.yaml')) as secret_file:
        app_creds = yaml.load(secret_file, Loader=yaml.SafeLoader)
    if 'consumer_key' not in app_creds or 'consumer_secret' not in app_creds:
        raise ValueError('secret.yml must contain consumer_key and consumer_secret')
    return app_creds


def get_bearer_token(consumer_key=None, consumer_secret=None):
    if consumer_key is None or consumer_secret is None:
        app_creds = get_app_creds()
        consumer_key = app_creds['consumer_key']
        consumer_secret = app_creds['consumer_secret']

    if not os.path.exists(BEARER_TOKEN_FILENAME):
        logging.info(f'Bearer token is not found in "{BEARER_TOKEN_FILENAME}", requesting a new one')
        oauth2_dance(consumer_key, consumer_secret, BEARER_TOKEN_FILENAME)
    else:
        logging.info(f'Reusing Bearer token from {BEARER_TOKEN_FILENAME}')

    with open(BEARER_TOKEN_FILENAME) as bearer_token_file:
        return bearer_token_file.read().rstrip()


def load_tweets_page(twitter_client, tweets_per_page, search_params):
    logging.debug('Making search request with params: {}'.format(search_params))
    result = twitter_client.search.tweets(
        **{
            'lang': 'en',

            **search_params,

            'count': tweets_per_page
        }
    )
    logging.debug(f'Got {len(result["statuses"])} tweets in response')
    return result


class TweetsLoader:

    def __init__(self, tweets_per_page=15, **search_params):
        self.twitter = Twitter(
            auth=OAuth2(bearer_token=get_bearer_token()),
            retry=100
        )
        self.tweets_per_page = tweets_per_page
        self.search_params = search_params
        self.since_id = None

    def load_tweets_iter(self, number_of_pages):
        if number_of_pages > 450:
            raise ValueError('Cannot make more than 450 requests in 15 mins!')

        min_id = None
        max_ids = []
        search_params = self.search_params.copy()
        if self.since_id is not None:
            search_params['since_id'] = self.since_id

        while number_of_pages > 0:
            if min_id is not None:
                search_params['max_id'] = min_id

            result = load_tweets_page(self.twitter, self.tweets_per_page, search_params)
            yield result

            if not result['statuses']:
                logging.info('No more tweets, stop loading results')
                break

            number_of_pages -= 1
            min_id = min(x['id'] for x in result['statuses']) - 1
            max_ids.append(result['search_metadata']['max_id'])

        self.since_id = max(max_ids)
        logging.info(f'Loaded tweets up to {self.since_id}')

    def load_tweets(self, number_of_pages):
        tweets = list(self.load_tweets_iter(number_of_pages))
        statuses = list(itertools.chain.from_iterable(x['statuses'] for x in tweets))
        search_metadata = [x['search_metadata'] for x in tweets]
        return statuses, search_metadata

    def stream_tweets(self, number_of_pages, metadata_store=None, max_errors_encountered=10):
        errors_encountered = 0
        while True:
            try:
                for result in self.load_tweets_iter(number_of_pages):
                    if metadata_store is not None:
                        metadata_store.append(result['search_metadata'])
                    yield from result['statuses']
            except TwitterHTTPError as exc:
                logging.exception('Twitter exception')
                errors_encountered += 1
                if errors_encountered > max_errors_encountered:
                    raise RuntimeError(f'Failed to reconnect: keep getting Twitter error {exc}')
            backoff = 2 ** errors_encountered
            logging.debug(f'Sleeping for {backoff} seconds before the next request')
            time.sleep(backoff)
