#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# basically adopted and modified from: https://gist.github.com/xamox/bfc4c610fd449debb3aa341863145bea

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.convert_timestamp import epoch_to_dt
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
from collections import deque
import time


DATABASE = 'mta_delays_dev'
COLLECTION = 'mta_delays_processed'
TIMEOUT = 60

esclient = Elasticsearch()
mgclient = MongoClient()
db = mgclient[DATABASE]
col = db[COLLECTION]

def delete_index():
    if esclient.indices.exists(index=DATABASE):
        print("{} exists, deleting...".format(DATABASE))
        esclient.indices.delete(index=DATABASE, ignore=[400, 404])
        print("{} deleted".format(DATABASE))


def index_es():

    delete_index()
    
    docs = []
    total = 0
    for data in col.find():
        data.pop('_id')
        data['formatted_ts'] = epoch_to_dt(data['timestamp_unix_mta'])
        doc = {
            "_index": DATABASE,
            "_type": COLLECTION,
            "_source": data
            }

        docs.append(doc)

        # Dump x number of objects at a time
        if len(docs) >= 50:
            total += 50
            deque(parallel_bulk(esclient, docs, request_timeout=TIMEOUT), maxlen=0)
            print('Indexed {} documents'.format(str(total)))
            docs = []

        time.sleep(.01)


if __name__ == '__main__':
    index_es()