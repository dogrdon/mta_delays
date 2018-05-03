#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.convert_timestamp import convert_timestamp
from kafka import KafkaProducer



def get_raw_delays_and_send_to_kafka():

	pass


if __name__ == '__main__':
	get_raw_and_send_to_kafka()
