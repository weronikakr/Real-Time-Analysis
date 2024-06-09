import json
import time 
import logging
import socket
import random
from datetime import datetime
from numpy.random import uniform, choice, randn
 
from random import random as r
 
import numpy as np
from confluent_kafka import Producer
 
 
SERVER = 'broker:9092'
KAFKA_TOPIC = 'heartrate'
LAG = 5
PROBABILITY_OUTLIER = 0.05
 
def create_producer():
    try:
        producer = Producer({
        "bootstrap.servers":SERVER,
        "client.id": socket.gethostname(),
        "enable.idempotence": True,
        "batch.size": 64000,
        "linger.ms":10,
        "acks": "all",
        "retries": 5,
        "delivery.timeout.ms":1000
        })
    except Exception as e:
        logging.exception("nie mogę utworzyć producenta")
        producer = None
    return producer
 
 
_id = 0 
producer = create_producer()
 
if producer is not None:
    while True:
        min_value = 60
        max_value = 120
        
        mean_main = 72
        std_dev_main = 1.5

        mean_occasional = 100
        std_dev_occasional = 2

        heart_rates_main = np.random.normal(mean_main, std_dev_main, size=9000)
        heart_rates_occasional = np.random.normal(mean_occasional, std_dev_occasional, size=1000)

        heart_rates_combined = np.concatenate((heart_rates_main, heart_rates_occasional))
        heart_rates_clipped = np.clip(heart_rates_combined, min_value, max_value)
        current_time = datetime.utcnow().isoformat()

        heart_rate = int(np.random.choice(heart_rates_clipped))
        data = {
            "time" : current_time,
            "heart_rate" : heart_rate
        }
        data = json.dumps(data).encode("utf-8")
        producer.produce(topic= KAFKA_TOPIC, value=data)
        #print(f"Sent: {data}")
        producer.flush()
        _id +=1 
        time.sleep(LAG)
