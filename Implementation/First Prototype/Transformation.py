# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 21:11:05 2023

@author: mahma
"""

from pyspark.sql.functions import *

from kafka import KafkaConsumer,KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import py4j
#from pyspark.streaming.kafka import kafkaUtils
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 pyspark-shell'
import findspark
findspark.init("C:\spark-3.3.1-bin-hadoop3")


def jsondeserializer(data):
    return json.loads(data)
# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("MyApp") \
    .config("spark.sql.warehouse.dir", "file:///C:/temp")\
    .getOrCreate()
sc=spark.sparkContext
ssc = StreamingContext(sc, 1)

# Define the schema of the data
schema = StructType([
    StructField("Index", StringType()),
    StructField("Product_Code", StringType()),
    StructField("Warehouse", StringType()),
    StructField("Product_Category", StringType()),
    StructField("Date", StringType()),
    StructField("Order_Demand", StringType())
])


# Read data from the input topic
df = spark.readStream \
    .format("kafka") \
    .option("startingOffsets", "earliest") \
    .option("kafka.bootstrap.servers", "[::1]:9092") \
    .option("subscribe", "topic_one") \
    .option("checkpointLocation", "file:///C:/temp") \
    .load()

# remove duplicates
distinct_df = df.selectExpr("cast(value as string) as json") \
                .select(from_json(col("json"), schema).alias("data")) \
                .select("data.*") \
                .distinct()
                
# Write the distinct data to the output topic
query = distinct_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "[::1]:9092") \
    .option("topic", "new") \
    .outputMode("append") \
    .option("checkpointLocation", "file:///C:/temp3") \
    .start()

query.awaitTermination()
