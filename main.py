import sys

from tweets_loader import TweetsLoader, configure_logging
from tweets_analyzer import TweetsAnalyzer
from textblob.download_corpora import download_all

configure_logging()

PAGES_PER_REQUEST = 5


def main():
    download_all()

    loader = TweetsLoader(q='brexit')
    analyzer = TweetsAnalyzer()

    scores_sum = 0
    tweets_count = 0
    for tweet_index, tweet in enumerate(loader.stream_tweets(PAGES_PER_REQUEST)):
        score = analyzer.analyze(tweet['full_text'])
        scores_sum += score
        tweets_count += 1
        if tweet_index % 100 == 0:
            print(f'Average score so far counted on {tweets_count} tweets: {scores_sum/tweets_count}')


if __name__ == '__main__':
    sys.exit(main())
