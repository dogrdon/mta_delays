#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connection import MongoConn
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import re
from bs4 import BeautifulSoup
import bson

sc = SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

def raw_text_processor():

    pass


def get_nearest_weather_obs(uts):
    '''
        Takes a unix timestamp and fetches the weather record
        closes in time to that and returns that record
    '''

    pass
# UDF....


df = spark.readStream.format("kafka") \
	.option("kafka.bootstrap.servers","localhost:9092") \
	.option("subscribe", "mta-delays") \
	.option("startingOffsets", "earliest").load()

jsonschema = StructType().add("timestamp", StringType()) \
                         .add("timestamp_unix", IntegerType()) \
                         .add("oid", StringType()) \
                         .add("lines", ArrayType(StructType() \
                            .add("line", StringType()) \
                            .add("status", StringType()) \
                            .add("raw_text", StringType())))

mta_stream = df.select(from_json(col("value") \
                                .cast("string"), jsonschema) \
                                .alias("parsed_mta_values"))

mta_data = mta_stream.select("parsed_mta_values.*")

mta_data_deduped = mta_data.dropDuplicates(['timestamp_unix'])

exploded_df = mta_data_deduped.select("*", explode("lines").alias("exploded_lines"))

split_df = exploded_df.select('timestamp', 'timestamp_unix', 'oid', 'exploded_lines.*')

# TODO: JOIN ON A WEATHER RECORD

# TODO: DEAL WITH RAW TEXT (opt for now)

# WRITE TO A KAFKA OUT STREAM FOR PICKUP

qry = split_df.writeStream.outputMode("append").format("console").start()
qry.awaitTermination()


