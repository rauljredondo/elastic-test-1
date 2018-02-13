from elasticsearch import Elasticsearch
from datetime import datetime
from TwitterAPI import TwitterAPI
import json
import _thread
import time
import os

user_fields = [
    'followers_count',
    'friends_count',
    'statuses_count',
    'location',
    'id_str',
    'name',
    'screen_name'
]

tweet_fields = [
    'created_at',
    'geo',
    'coordinates',
    'place',
    'lang',
    'text'
]

yahoo_woeid = {
    'Spain': 23424950,
    'Barcelona':753692,
    'Biscay':754542,
    'Madrid': 766273,
    'Seville':774508
}


api = TwitterAPI(os.environ['consumer_key'], os.environ['consumer_secret'], os.environ['access_token_key'], os.environ['access_token_secret'])

http_auth = (os.environ['elasticUser'], os.environ['elasticSectet'])
es = Elasticsearch([os.environ['elasticHost']], http_auth=http_auth)

def parse_trend_tweet(location, woeid):
    r = api.request('trends/place', {'id': woeid})
    tweet_volume_prev = 0
    for item in r.get_iterator():
        tweet = dict(
            location=location,
            woeid=woeid,
            post_date=datetime.now(),
            name=item['name']
        )

        tweet['promoted_content'] = item['promoted_content'] if 'promoted_content' in item and item['promoted_content'] else None
        if 'tweet_volume' in item and item['tweet_volume']:
            tweet['tweet_volume'] = item['tweet_volume']
            tweet_volume_prev = item['tweet_volume']
        else:
            tweet['tweet_volume'] = tweet_volume_prev * 0.9
            tweet_volume_prev *= 0.95

        print(tweet)
        time.sleep(0.5)
        es.index(index='mytwitter', doc_type='trend', body=tweet)

def get_trendings():
	for key, value in yahoo_woeid.items():
		_thread.start_new_thread(parse_trend_tweet, (key, value, ))
		time.sleep(1)

def main():
	get_trendings()

if __name__ == "__main__":
    main()






