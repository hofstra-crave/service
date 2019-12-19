import string

import numpy as np
from elasticsearch import Elasticsearch
from nltk import RegexpTokenizer
from nltk.corpus import stopwords
from textblob import TextBlob

from authTwitter import authTW

es = Elasticsearch([{'host': '10.22.12.229', 'port': 9200}], http_auth=('user', 'pkqcqbke5qPP'))


##
# Tweet processors
##

def map_tweets(biz):
    twitter_obj = authTW()
    return get_tweets_for_keyword(twitter_obj, biz["name"], biz["business_id"])


def get_sentiment(t):
    # using TextBlob, create an Object from the input tweet
    tb_object = TextBlob(t)

    # compute the sentiment
    if tb_object.sentiment.polarity > 0:
        return 'pos', 4.5
    elif tb_object.sentiment.polarity == 0:
        return 'neu', 3
    else:
        return 'neg', 2.5


def clean_tweet(tweet):
    text = tweet['text']

    slices = []
    # Strip out the urls.
    if 'urls' in tweet['entities']:
        for url in tweet['entities']['urls']:
            slices += [{'start': url['indices'][0], 'stop': url['indices'][1]}]

    # Strip out the hashtags.
    if 'hashtags' in tweet['entities']:
        for tag in tweet['entities']['hashtags']:
            slices += [{'start': tag['indices'][0], 'stop': tag['indices'][1]}]

    # Strip out the user mentions.
    if 'user_mentions' in tweet['entities']:
        for men in tweet['entities']['user_mentions']:
            slices += [{'start': men['indices'][0], 'stop': men['indices'][1]}]

    # Strip out the media.
    if 'media' in tweet['entities']:
        for med in tweet['entities']['media']:
            slices += [{'start': med['indices'][0], 'stop': med['indices'][1]}]

    # Strip out the symbols.
    if 'symbols' in tweet['entities']:
        for sym in tweet['entities']['symbols']:
            slices += [{'start': sym['indices'][0], 'stop': sym['indices'][1]}]

    # Sort the slices from highest start to lowest.
    slices = sorted(slices, key=lambda x: -x['start'])

    # No offsets, since we're sorted from highest to lowest.
    for s in slices:
        text = text[:s['start']] + text[s['stop']:]

    # Remove emojis
    text = text.encode('ascii', 'ignore').decode('ascii')

    # Remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))

    # Remove extraneous spacing
    text = ' '.join(text.splitlines())
    text = ' '.join(text.split())

    # Remove retweet symbol
    if text[0:3] == 'RT ':
        text = text[3:]

    return text


def get_tweets_for_keyword(t_obj, keyword, id):
    # use the twitter api to get the tweets
    search_results = t_obj.search.tweets(q=keyword, lang='en', count=150)

    # filter the json results just to status
    statuses = search_results['statuses']

    # iterate through the status to get the tweet text and id, save only unique tweets
    tw_list = []
    tw_set = set()
    for tw in statuses:
        tw_text = clean_tweet(tw)
        tw_sentiment, tw_rating = get_sentiment(tw_text)

        # Add to list if
        # - Not a duplicate
        # - Not too short
        # - Has trailing words (...)
        if tw_text not in tw_set and len(tw_text) > 10 and '…' not in tw_text:
            tw_list.append({
                "business_id": id,
                "sentiment": tw_sentiment,
                "rating": tw_rating,
                "text": tw_text
            })
            tw_set.add(tw_text)

    return np.array(tw_list)


##
# Yelp processors
##

def clean_text(t):
    sentence = t.lower()
    tokenizer = RegexpTokenizer(r'\w+')
    tokens = tokenizer.tokenize(sentence)
    filtered_words = filter(lambda token: token not in stopwords.words('english'), tokens)
    return " ".join(filtered_words)


def get_simple_sentiment(stars):
    if stars <= 2.5:
        return 'neg'
    if stars <= 3.5:
        return 'neu'
    else:
        return 'pos'


def get_yelp_review():
    res = es.search(index='raw_yelp_review', size=10000)
    reviews = list(map(lambda x: {
        "business_id": x['_source']['business_id'],
        "sentiment": get_simple_sentiment(x['_source']['stars']),
        "rating": x['_source']['stars'],
        "text": clean_text(x['_source']['text'])
    }, res['hits']['hits']))

    return np.array(reviews)
