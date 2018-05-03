#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka import KafkaProducer
from connection import MongoConn
import datetime
import json
from bson.json_util import dumps


MTA_DELAYS_IN_KAFKA_TOPIC = 'mta-delays'

producer = KafkaProducer(bootstrap_servers='localhost:9092', 
					     value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def get_raw_delays_and_send_to_kafka():
	new_last_record_timestamp = None

	last_batch_store_client = MongoConn('mta_delays_dev', 'last_batch_end')
	mta_delays_client = MongoConn('mta_delays_dev', 'mta_delays')

	last_record = list(last_batch_store_client.get_last_record('last_timestamp_unix'))
	if last_record == []:
		# get all the records
		records = list(mta_delays_client.get_all_records())

	else:
		# only get records since last timestamp
		last_record_timestamp = last_record[0]['last_timestamp_unix']
		records = list(mta_delays_client.get_records_gt('timestamp_unix', last_record_timestamp))


	for record in records:
		# send record to kafka topic
		try:
			serialized_record = dumps(record)
			producer.send(MTA_DELAYS_IN_KAFKA_TOPIC, serialized_record)
			new_last_record_timestamp = record['timestamp_unix']
			
		except Exception as e:
			print("Tried sending {} from {} to topic and it failed: {}, Quiting for now, will start from where left off".format(record['_id'], record['timestamp'], e))
			break

	if new_last_record_timestamp is not None:
		update_last_batch = {}
		update_last_batch['last_timestamp_unix'] = new_last_record_timestamp
		update_last_batch['run_complete_timestamp'] = datetime.datetime.utcnow()
		print('saving last_batch_run: {}'.format(new_last_record_timestamp))
		last_batch_store_client.save_record(update_last_batch)



if __name__ == '__main__':
	get_raw_delays_and_send_to_kafka()
