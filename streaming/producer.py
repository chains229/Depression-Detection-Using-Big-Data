import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import logging
from time import sleep
import re
import json

kafka_server = 'localhost:9092'

#admin_client = KafkaAdminClient(bootstrap_servers=kafka_server, client_id='producer')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
topic_name = "depression_detection"
topic = NewTopic(name=topic_name, replication_factor=1, num_partitions=1)
#admin_client.create_topics(new_topics=[topic])

producer = KafkaProducer(bootstrap_servers=kafka_server,
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

df = pd.read_csv('streaming/stressed_anxious.csv')
for index, row in df.iterrows():
    message = {
        "_c0": int(index),
        "text": row['text'] 
    } 
    producer.send(topic_name, value=message)
    logger.info(f"Produced message: {message}")
    sleep(5)
