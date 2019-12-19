import math

import numpy as np
from elasticsearch import Elasticsearch
from nltk.tokenize import word_tokenize

es = Elasticsearch([{'host': '10.22.12.229', 'port': 9200}], http_auth=('user', 'pkqcqbke5qPP'))


def compare_businesses(biz_1, biz_2):
    # tokenize business categories
    try:
        biz_1_set = set(word_tokenize(biz_1['categories']))
        biz_2_set = set(word_tokenize(biz_2['categories']))
    except:
        return 0

    # form a set containing categories of both businesses
    l1 = []
    l2 = []
    union_vector = biz_1_set.union(biz_2_set)
    for w in union_vector:
        if w in biz_1_set:
            l1.append(1)
        else:
            l1.append(0)
        if w in biz_2_set:
            l2.append(1)
        else:
            l2.append(0)
    c = 0

    # cosine similarity between categories
    for i in range(len(union_vector)):
        c += l1[i] * l2[i]
    cat_similarity = c / float((sum(l1) * sum(l2)) ** 0.5)

    # euclidean distance similarity for ratings
    rating_similarity = 1 / (1 + math.sqrt((biz_1['stars'] - biz_2['stars']) ** 2))

    return cat_similarity * rating_similarity


def get_similar_businesses():
    res = es.search(index='raw_yelp_business', size=500)
    businesses = list(map(lambda x: x['_source'], res['hits']['hits']))

    master_businesses = []

    for i in range(len(businesses)):

        if businesses[i]["categories"] is None:
            continue

        highest_rating = 0
        highest_rating_index = i

        for j in range(len(businesses)):
            if i != j:
                rating = compare_businesses(businesses[i], businesses[j])
                if rating > highest_rating:
                    highest_rating = rating
                    highest_rating_index = j

        biz = {
            "name": businesses[i]["name"],
            "address": businesses[i]["address"],
            "description": businesses[i]["categories"],
            "business_id": businesses[i]["business_id"],
            "similar_id": businesses[highest_rating_index]["business_id"]
        }
        master_businesses.append(biz)

    return np.array(master_businesses)
