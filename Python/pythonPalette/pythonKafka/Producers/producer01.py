from kafka import KafkaProducer
from time import sleep


def serializer(message):
    return bytes(str(message), 'UTF-8')


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

for i in range(5):
    producer.send('first_topic', value=i+1)
    producer.send('second_topic', value=i + 11)
    sleep(1)
