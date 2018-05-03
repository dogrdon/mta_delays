#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import xml.etree.ElementTree as ET
import requests
from connection import MongoConn
from utils.convert_timestamp import convert_timestamp


def etree_to_dict(t):
    return {t.tag : map(etree_to_dict, t.iterchildren()) or t.text}

def parse_xml(r):
	subway_status = {}
	subway_status['timestamp'] = r.find('./timestamp').text
	subway_status['timestamp_unix'] = convert_timestamp(subway_status['timestamp'])
	subwaylines = r.findall("./subway/line")

	subway_status['lines'] = []
	for i in subwaylines:
		line_record = {}
		line_record['line'] = i.find('name').text
		line_record['status'] = i.find('status').text
		line_record['raw_text'] = i.find('text').text
		subway_status['lines'].append(line_record)

		# strip entries to clean things up

		for k, v in line_record.items():
			if v is not None:
				v.strip()
	
	return subway_status

def ingest_mta_delays():	
	conn = MongoConn('mta_delays_dev', 'mta_delays')
	SOURCE = "http://web.mta.info/status/serviceStatus.txt"
	res = requests.get(SOURCE)
	xmlstring = res.text
	root = ET.fromstring(xmlstring)

	status_data = parse_xml(root)

	'''Send to raw store'''
	conn.save_record(status_data)
	
if __name__ == '__main__':
	ingest_mta_delays()
