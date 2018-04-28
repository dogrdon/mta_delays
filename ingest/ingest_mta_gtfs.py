#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# https://developers.google.com/transit/gtfs-realtime/examples/python-sample
# pip3 install --upgrade --user gtfs-realtime-bindings

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from google.transit import gtfs_realtime_pb2
import requests
from google.protobuf.json_format import MessageToJson
import json
from connection import MongoConn


api_key = 'cd11239777abea125dc7d109a6f4c29f'

feed = gtfs_realtime_pb2.FeedMessage()

feeds = {
	"irt":1,
	"ace":26
}

def get_gtfs_url(feed):
	return "http://datamine.mta.info/mta_esi.php?key={api_key}&feed_id={feed_id}".format(api_key=api_key, feed_id=feeds[feed])


def filter_messages(msgs, target_line):
	filtered_msgs = []
	for msg in msgs:
		msg_keys = msg.keys()
		if "tripUpdate" in msg_keys:
			routeId = msg['tripUpdate']['trip']['routeId']
			if routeId == target_line:
				filtered_msgs.append(msg)
		elif "vehicle" in msg_keys:
			routeId = msg['vehicle']['trip']['routeId']
			if routeId == target_line:
				filtered_msgs.append(msg)
		else:
			print("Message is an alert, adding it to the output anyway: {}".format(msg))
			filtered_msgs.append(msg)
			
	return filtered_msgs


def get_gtfs_data(line, target_line):
	data = {}
	gtfs_raw = requests.get(get_gtfs_url(line)).content
	feed.ParseFromString(gtfs_raw)
	data['timestamp'] = feed.header.timestamp
	data['line'] = line
	data['route'] = target_line
	messages = [json.loads(MessageToJson(entity)) for entity in feed.entity]
	filtered_messages = filter_messages(messages, target_line)	
	data['messages']  = filtered_messages

	return data


def ingest_mta_gtfs():
	conn = MongoConn('mta_delays_dev', 'mta_realtime')
	irt_gtfs_data = get_gtfs_data("irt", "1")
	ace_gtfs_data = get_gtfs_data("ace", "A")
	conn.save_record(irt_gtfs_data)
	conn.save_record(ace_gtfs_data)



if __name__ == '__main__':
	ingest_mta_gtfs()

