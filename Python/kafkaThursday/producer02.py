from kafka import KafkaProducer
import json
from time import sleep


def serializer(message):
    return json.dumps(message, sort_keys=True, indent=4).encode('utf-8')


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

for i in range(5):
    producer.send('first_topic', value={'int': i + 1})
    producer.send('second_topic', value={'int': i + 11})
    sleep(1)
