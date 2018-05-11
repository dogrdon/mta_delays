#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''Remember to set unique constraint index on db so duplicates are not inserted:
	db.mta_delays_processed.createIndex( { "line": 1, "timestamp_unix_mta": 1 }, { unique: true } )
'''

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka import KafkaConsumer
import json
from bson.json_util import dumps
import pymongo


MTA_DELAYS_OUT_KAFKA_TOPIC = 'mta-delays-processed'

consumer = KafkaConsumer(MTA_DELAYS_OUT_KAFKA_TOPIC,
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))


def consume_processed_mta_delays():

    client = pymongo.MongoClient('localhost', 27017)
    db = client['mta_delays_dev']
    collection = db['mta_delays_processed']

    for message in consumer:
        msg = message.value
        
        # delete the original mongodb keys so things don't get confused for reloading back to another mongo collection
        if '_id' in msg.keys():
            del msg['_id']
        if 'oid' in msg.keys():
            del msg['oid']

        # method for avoiding duplicate errors from: https://stackoverflow.com/questions/44838280/how-to-ignore-duplicate-key-errors-safely-using-insert-many
        try:
        	collection.insert_one(msg)
        except pymongo.errors.DuplicateKeyError as e:	
        	print("Dupe: {}".format(e))
        	continue

if __name__ == '__main__':
    consume_processed_mta_delays()