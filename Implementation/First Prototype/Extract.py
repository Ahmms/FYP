# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 10:23:02 2023

@author: usman
"""
from kafka import KafkaProducer,KafkaConsumer
import csv
import json

columns = ["Index", "Product_Code", "Warehouse", "Product_Category", "Date", "Order_Demand"]

def jsonserializer(data):
    return json.dumps(dict(zip(columns,data))).encode("utf-8")
    
producer=KafkaProducer(bootstrap_servers=['[::1]:9092'],value_serializer=jsonserializer)
with open('demo.csv',mode='r') as file:
    reader=csv.reader(file)
    #next(reader)
    for row in reader:
        producer.send('topic_one',row)
producer.flush()