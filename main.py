from elasticsearch import Elasticsearch
from datetime import datetime
from TwitterAPI import TwitterAPI
import config
import json
import _thread
import time


api = TwitterAPI(config.consumer_key, config.consumer_secret, config.access_token_key, config.access_token_secret)

http_auth = (config.elasticUser, config.elasticSectet)
es = Elasticsearch([config.elasticHost],
                   http_auth=http_auth)

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


def process_tweet(item, topic):
    tweet = {'hastags': []}
    user = {}
    for field in user_fields:
        if field in item['user'] and item['user'][field]:
            user[field] = item['user'][field]

    for field in tweet_fields:
        if field in item and item[field]:
            tweet[field] = item[field]

    tweet['user_id'] = item['user']['id_str']
    tweet['post_date'] = datetime.now()
    user['post_date'] = datetime.now()

    for word in topic:
        if word in item['text']:
            tweet['topic'] = word
            break

    if 'hastags' in item['entities']:
        for elem in item['entities']['hastags']:
            tweet['hastags'].append(elem)

    #tweet = json.dumps(tweet, ensure_ascii=False)
    #user = json.dumps(user, ensure_ascii=False)
    es.index(index='twitter_status', doc_type='_doc', body=tweet)
    es.index(index='twitter_user', doc_type='_doc', body=user, id=item['user']['id_str'])

def stream_topic(topic):
    r = api.request('statuses/filter', {'track': topic})
    time.sleep(0.5)
    for item in r.get_iterator():
        try:
            _thread.start_new_thread(process_tweet, (item, topic, ))
        except:
            print("Error: unable to start thread at tweet level")


def main():
    words = ['Bitcoin', 'Litecoin', 'Ethereum']
    stream_topic(words)
    """
    for word in words:
        try:
            _thread.start_new_thread(stream_topic, (word, ))
        except:
            print("Error: unable to start thread at stream level")
    """

if __name__ == "__main__":
    main()






