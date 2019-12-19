import numpy as np
import pymysql
from elasticsearch import Elasticsearch

import reviews
import similarity

es = Elasticsearch([{'host': '10.22.12.229', 'port': 9200}], http_auth=('user', 'pkqcqbke5qPP'))
db = pymysql.connect(host='10.22.12.131',
                     user='root',
                     password='SCL$Xdat4ML',
                     db='Team Crave',
                     charset='utf8mb4',
                     cursorclass=pymysql.cursors.DictCursor)


def post_review(r):
    try:
        with db.cursor() as cursor:
            sql = "INSERT INTO `Reviewz` (`Restaurant_ID`, `Review_Text`, `Rating`, `sentiment`) VALUES(%s, %s, %s, %s)"
            cursor.execute(sql, (r["business_id"], r["text"], r["rating"], r["sentiment"]))
        db.commit()
    finally:
        cursor.close()


def post_restaurant(r):
    try:
        with db.cursor() as cursor:
            sql = "INSERT INTO `Restaurantz` (`Restaurant_ID`, `Name`, `Address`, `Description`, `SimilarID`) VALUES(%s, %s, %s, %s, %s)"
            cursor.execute(sql, (r["business_id"], r["name"], r["address"], r["description"], r["similar_id"]))
        db.commit()
    finally:
        cursor.close()


def main():
    res = es.search(index='raw_yelp_business', size=500)
    businesses = list(map(lambda x: x['_source'], res['hits']['hits']))

    master_reviews = []
    master_reviews = np.concatenate((master_reviews, reviews.get_yelp_review()), axis=0)
    for biz in businesses:
        master_reviews = np.concatenate((master_reviews, reviews.map_tweets(biz)), axis=0)

    # Post Reviews
    for review in reviews.get_yelp_review():
        post_review(review)

    # Post restaurants
    for restaurant in similarity.get_similar_businesses():
        post_restaurant(restaurant)


main()
