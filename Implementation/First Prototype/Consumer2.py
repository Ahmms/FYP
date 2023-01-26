# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 14:38:37 2023

@author: mahma
"""

from kafka import KafkaConsumer
consumer=KafkaConsumer(bootstrap_servers=['[::1]:9092']
                       ,auto_offset_reset='earliest')
consumer.subscribe(['topic_automation_2'])
for msg in consumer:
    print(msg.value)