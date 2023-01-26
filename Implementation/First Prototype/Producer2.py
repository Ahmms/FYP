# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 14:28:54 2023

@author: mahma
"""

from pandas import read_csv
from kafka import KafkaProducer
import time

# create kafka producer
producer = KafkaProducer(bootstrap_servers='[::1]:9092')

# read csv file
df = read_csv("demo.csv")
prev_row_count = len(df)

while True:
    # check for new rows
    df = read_csv("demo.csv")
    current_row_count = len(df)
    if current_row_count >= prev_row_count:
        print("nice")
        new_rows = df[prev_row_count:]
        for index, row in new_rows.iterrows():
            # send data to kafka topic
            producer.send('topic_automation_2', value=row.to_json().encode())
        prev_row_count = current_row_count
    print("done")
    time.sleep(10)
