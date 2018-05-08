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
    --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/mta_delays_dev.mta_delays_processed"
'''


def raw_text_processor():

    pass


def round_down_time(uts):
    '''
        Takes a unix timestamp and rounds it down to the nearest unit of time
        can be configured by seconds
    '''
    minutes=2
    return int(uts - (uts % int(minutes * 60)))

roundDownTimeUDF = udf(round_down_time, IntegerType())


def get_nearest_weather_obs(uts):
    '''
        Takes a unix timestamp and fetches the weather record
        closest in time to that from the weather df loaded already and returns that record
    '''

    df_res = weather_df.filter(weather_df.timestamp_unix > uts).sort(col('timestamp_unix').asc()).limit(1).show()
    return resstr

nearestWeatherUDF = udf(get_nearest_weather_obs, StringType())


weather_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
weather_df = weather_df.withColumnRenamed("time", "timestamp_unix_weather")
weather_df = weather_df.withColumn('rounded_timestamp', roundDownTimeUDF(col('timestamp_unix_weather')))

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

full_mta_df = split_df.withColumn('rounded_timestamp', roundDownTimeUDF(col('timestamp_unix')))

# JOIN ON A WEATHER RECORD - 
# as above, we've rounded the time down to the next 2 minutes so that we have more records joined

join_weather_df = full_mta_df.join(weather_df, "rounded_timestamp", "left" ).withColumnRenamed("timestamp_unix", "timestamp_unix_mta")

# TODO: DEAL WITH RAW TEXT (opt, for now just return text for DELAYS or SERVICE CHANGE status)

full_data_pruned_df = join_weather_df.withColumn('delay_text', when((join_weather_df.status=="GOOD SERVICE") | (join_weather_df.status=="PLANNED WORK"), None).otherwise(trim(join_weather_df.raw_text)))\
                                     .drop(join_weather_df.raw_text)


#qry = full_data_pruned_df.writeStream.outputMode("append").format("console").start()
#qry.awaitTermination()

# WRITE TO A KAFKA OUT STREAM FOR PICKUP

df_cols = list(full_data_pruned_df.columns)

print(df_cols)

ds = full_data_pruned_df \
  .select(to_json(struct("*")).alias("value")) \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "mta-delays-processed") \
  .option("checkpointLocation", "/data/sparkCheckpoints/mta-delays-processed") \
  .outputMode("append") \
  .start()

ds.awaitTermination()
