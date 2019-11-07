import re

from textblob import TextBlob


class TweetsAnalyzer:

    @staticmethod
    def _clean_tweet(tweet_text):
        """
        Clean tweet text by removing links, special characters.
        """
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w+:\ / \ / \S+) ", " ", tweet_text).split())

    def analyze(self, tweet_text):
        analysis = TextBlob(self._clean_tweet(tweet_text))
        return analysis.sentiment.polarity
