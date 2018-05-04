#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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


df = spark.readStream.format("kafka") \
	.option("kafka.bootstrap.servers","localhost:9092") \
	.option("subscribe", "mta-delays") \
	.option("startingOffsets", "earliest").load()


jsonschema = StructType().add("timestamp", StringType()) \
                         .add("timestamp_unix", IntegerType()) \
                         .add("_id", StructType() \
                            .add("$oid", StringType()) \
                         .add("lines", ArrayType(StructType() \
                            .add("line", StringType()) \
                            .add("status", StringType()) \
                            .add("raw_text", StringType()))))


mta_stream = df.select(from_json(col("value") \
                                .cast("string"), jsonschema) \
                                .alias("parsed_mta_values"))

mta_data = mta_stream.select("parsed_mta_values.*")

print(mta_stream.printSchema)

qry = mta_data.writeStream.outputMode("append").format("console").start()
qry.awaitTermination()
