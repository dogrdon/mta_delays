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

def signal_problems(delay_text):
  ''' Take the raw text from any delays contains the string signal, True, else false.
      This of course could be way more robust, but I think for right now this should be
      sufficient for getting a rought idea.
  '''
  signal_problems = 0
  if delay_text is not None:
    if 'signal' in delay_text.lower():
      signal_problems = 1
  
  return signal_problems

signalProblemsUDF = udf(signal_problems, IntegerType())

def scale_probabilities(proba):
  '''
    Some values are probabilites, since we want to chart them better, lets multiply them by 100
  '''
  scaled_proba = None
  if proba is not None:
    scaled_proba = proba * 100

  return scaled_proba


scaledProbaUDF = udf(scale_probabilities, DoubleType())

def rain_or_not(summary):
  '''
    Flag if it's raining or not, let's just skip drizzle for now
  '''
  rain = 0
  if summary is not None:
    if 'rain' in summary.lower():
      rain = 1
  return rain

rainOrNotUDF = udf(rain_or_not, IntegerType())

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

full_data_coded_df = full_data_pruned_df.withColumn('delay', when(full_data_pruned_df.status=="DELAYS", 1).otherwise(0)) \
                                        .withColumn('service_change', when(full_data_pruned_df.status=="SERVICE CHANGE", 1).otherwise(0)) \
                                        .withColumn('raining', rainOrNotUDF(col('summary'))) \
                                        .withColumn('precip_proba_scaled', scaledProbaUDF(col('precipProbability'))) \
                                        .withColumn('precip_inten_scaled', scaledProbaUDF(col('precipIntensity'))) \
                                        .withColumn('humidity_scaled', scaledProbaUDF(col('humidity'))) \
                                        .withColumn('signal_problem', signalProblemsUDF(col('delay_text')))


#qry = full_data_pruned_df.writeStream.outputMode("append").format("console").start()
#qry.awaitTermination()

# WRITE TO A KAFKA OUT STREAM FOR PICKUP

ds = full_data_coded_df \
  .select(to_json(struct("*")).alias("value")) \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "mta-delays-processed") \
  .option("checkpointLocation", "/data/sparkCheckpoints/mta-delays-processed") \
  .outputMode("append") \
  .start()

ds.awaitTermination()
