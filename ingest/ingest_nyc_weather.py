#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import requests
from connection import MongoConn

def parse_data(data):
	summary_record = {}
	
	current = data['currently']
	for k, v in current.items():
		summary_record[k] = v

	summary_record['minute_status'] = data['minutely']['summary']
	summary_record['latitude'] = data['latitude']
	summary_record['longitude'] = data['longitude']
	summary_record['timezone'] = data['timezone']

	return summary_record


def ingest_nyc_weather():
	conn = MongoConn('mta_delays_dev', 'nyc_weather')
	SOURCE = "https://api.darksky.net/forecast/2b34107e885f4e81dea4b0389c54cad4/40.730610,-73.935242"
	res = requests.get(SOURCE)
	data = json.loads(res.text)

	record = parse_data(data)	

	''' Send record to first raw storage '''
	conn.save_record(record)

if __name__ == '__main__':
	ingest_nyc_weather()
