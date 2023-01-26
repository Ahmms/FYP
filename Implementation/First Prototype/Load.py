# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 21:01:43 2023

@author: mahma
"""
from cassandra.cluster import Cluster
from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
import findspark
findspark.init("C:\spark")

columns = ["Index", "Product_Code", "Warehouse", "Product_Category", "Date", "Order_Demand"]
def ConvertToInt(data):
    data[0]=int(data[0])
    data[5]=int(data[5])
    return data
def jsondeserializer(data):
    data = json.loads(data.decode("utf-8"))
    return [data[column] for column in columns]
consumer=KafkaConsumer(bootstrap_servers=['[::1]:9092'],value_deserializer=jsondeserializer
                       ,auto_offset_reset='earliest')
consumer.subscribe(['new'])

#creating connection with cassandra keyspace
cluster = Cluster(['127.0.0.1'], port=9042)

session = cluster.connect(keyspace="industry_data")

createTableQuery = "CREATE TABLE IF NOT EXISTS Data( Index1 INT, Product_Code TEXT, Warehouse TEXT, Product_Category TEXT, Date TEXT,Order_Demand int, PRIMARY KEY (Index1));"

insertQuery = "INSERT INTO Data(Index1,Product_Code,WareHouse,Product_Category,Date,Order_Demand) VALUES(%s,%s, %s, %s, %s, %s) ;"
session.execute(createTableQuery)
print("!-!-!- SUCCESS : connected to cassandra and table created")

try:
    for msg in consumer:
        value = ConvertToInt(msg.value)
        if(value == "end"):
            cluster.shutdown()
            sys.exit()
        #we need to store the value in cassandra
        try:
            print(value)
            #now we need to insert the data
            session.execute(insertQuery, value)
            
            
        except:
            print("------OOPS : error connecting to cassandra")
            
       
except KeyboardInterrupt:
    cluster.shutdown()
    sys.exit()
  
    
#['Product_0796', 'Whse_J', 'Category_001', '2012/8/3', '5 '] --> Deserialize(msg)
#industry_data     0       1094729       None       b'["Product_0704", "Whse_J", "Category_001", "2016/6/27", "4 "]'