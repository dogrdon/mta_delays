#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from pymongo import MongoClient, DESCENDING



class MongoConn(object):
	"""Mongo Connection interface"""
	def __init__(self, database, collection, port=27017):
		super(MongoConn, self).__init__()
		self.database = database
		self.collection = collection
		client = MongoClient(port=port)
		self.db = client[database]
		self.collection = self.db[collection]
		
	def save_record(self, record):
		# save one document
		self.collection.insert_one(record)

	def save_records(self, records):
		# save one or more documents
		self.collection.insert_many(records)

	def get_last_record(self, field):
		return self.collection.find().sort( field, DESCENDING ).limit(1)
		
	def get_all_records(self):
		return self.collection.find()

	def get_records_gt(self, field, val):
		return self.collection.find({field:{'$gt':val}})

	def get_one_record(self, field=None, val=None):
		return self.collection.find_one({field:val})

	def update_one(self, record, upsert=True):
		return self.collection.update_one(record)