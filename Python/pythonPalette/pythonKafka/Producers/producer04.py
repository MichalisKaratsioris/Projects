import random
from kafka import KafkaProducer
import json
from time import sleep


def serializer(message):
    return json.dumps(message, sort_keys=True, indent=4).encode('utf-8')


def create_message():
    n = random.choice(list(range(1, 11)))
    if n % 2 == 0:
        return {'first': n}
    else:
        return {'second': n}


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

for i in range(10):
    message = create_message()
    n = random.choice(list(range(1, 11)))
    if n % 2 == 0:
        producer.send('first_topic', value=message)
    else:
        producer.send('second_topic', value=message)
    sleep(1)