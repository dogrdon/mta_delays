#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# basically adopted from: https://gist.github.com/xamox/bfc4c610fd449debb3aa341863145bea

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.convert_timestamp import epoch_to_dt

DATABASE = 'mta_delays_dev'
COLLECTION = 'mta_delays_processed'


from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
from collections import deque
import time
client = Elasticsearch()
mgclient = MongoClient()
db = mgclient[DATABASE]
col = db[COLLECTION]

docs = []
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
    if len(docs) >= 100:
    	deque(parallel_bulk(client, docs), maxlen=0)
    	docs = []
    time.sleep(.01)