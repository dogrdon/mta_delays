#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import re
from bs4 import BeautifulSoup
from bson.json_util import dumps

sc = SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

'''to connect to the mongodb weather df, we need to include the following in the spark-submit command:

    --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.2 \
    --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/mta_delays_dev.nyc_weather?readPreference=primaryPreferred"

'''
weather_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
weather_df = weather_df.withColumnRenamed("time", "timestamp_unix")

def raw_text_processor():

    pass



def get_nearest_weather_obs(uts):
    '''
        Takes a unix timestamp and fetches the weather record
        closest in time to that from the weather df loaded already and returns that record
    '''

    df_res = weather_df.filter(weather_df.timestamp_unix > uts).sort(col('timestamp_unix').asc()).limit(1).show()
    return resstr

nearestWeatherUDF = udf(get_nearest_weather_obs, StringType())

df = spark.readStream.format("kafka") \
	.option("kafka.bootstrap.servers","localhost:9092")\
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

join_weather_df = weather_df.join(mta_data_deduped, "timestamp_unix", "right" )

# TODO: DEAL WITH RAW TEXT (opt for now)

# WRITE TO A KAFKA OUT STREAM FOR PICKUP

qry = join_weather_df.writeStream.outputMode("append").format("console").start()
qry.awaitTermination()

#weather_df.printSchema()
#split_df.printSchema()
#join_weather_df.printSchema()

