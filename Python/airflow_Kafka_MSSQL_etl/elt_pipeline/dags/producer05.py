from kafka import KafkaProducer
import json
from time import sleep


def serializer(message):
    return json.dumps(message, sort_keys=True, indent=4).encode('utf-8')


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

topics = ['sports_api', 'scores_api', 'odds_api']
files = ['sports1.json', 'scores.json', 'odds.json']

for i in range(len(topics)):
    with open(files[i], 'r') as f:
        json_object = json.load(f)
    producer_value = json_object
    producer.send(topics[i], value=producer_value)
    sleep(1)
