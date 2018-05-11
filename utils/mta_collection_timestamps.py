#!/usr/bin/env python3
# -*- coding: utf-8 -*-


'''
    one time fix to create a `timestamp_unix` field for our raw collection 
    of mta delays in mongodb.

    Should not be necessary going forward, this is once only use code
'''

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pymongo import MongoClient
from utils.convert_timestamp import convert_timestamp



client = MongoClient('localhost', 27017)


db = client.mta_delays_dev
collection = db.mta_delays

records = collection.find()


for record in records:
	rec_to_update = collection.find_one({'_id':record['_id']})
	#print(rec_to_update['timestamp'], rec_to_update['timestamp_unix'])
	rec_to_update['timestamp_unix'] = convert_timestamp(rec_to_update['timestamp'])
	collection.update_one({'_id':record['_id']}, {"$set": rec_to_update}, upsert=False)



