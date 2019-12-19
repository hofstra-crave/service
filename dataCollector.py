import json
import os

from elasticsearch import Elasticsearch

from authTwitter import authTW

PATH_YELP = './dataset/'

es = Elasticsearch([{'host': '10.22.12.229', 'port': 9200}], http_auth=('user', 'pkqcqbke5qPP'))


# Given a directory, bulk index valid json files
def load_json(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            with open(filename, 'r') as open_file:
                yield json.load(open_file)


# Index a given JSON file to ElasticSearch
def index_json(json_file, index, doc_type):
    with open(json_file) as raw_data:
        for json_line in raw_data:
            json_obj = json.loads(json_line)
            es.index(index=index, doc_type=doc_type, body=json_obj)


# Index static Yelp datasets
# N.B. Should only be run once!
def index_yelp():
    index_json(PATH_YELP + 'tip.json', 'raw_yelp_tip', 'tip')
    index_json(PATH_YELP + 'review.json', 'raw_yelp_tip', 'review')
    index_json(PATH_YELP + 'business.json', 'raw_yelp_tip', 'business')


# Syncs new tweets for previously indexed restaurants
def sync_tweets():
    res = es.search(index='raw_yelp_business', size=50)
    info = lambda raw: {
        "id": raw['_source']['business_id'],
        "name": raw['_source']['name']
    }

    businesses = list(map(info, res['hits']['hits']))
    for business in businesses:
        index_tweets(business['name'], business['id'])


# Search and index new tweets for a given business
def index_tweets(r_name, r_id):
    twitter_obj = authTW()
    tweets = twitter_obj.search.tweets(q=f'"${r_name}"', lang='en', count=50)

    for tweet in tweets['statuses']:
        tweet['business_id'] = r_id
        tweet['business_name'] = r_name
        es.index(index='raw_tweets', doc_type='tweet', id=tweet['id'], body=tweet)


sync_tweets()
