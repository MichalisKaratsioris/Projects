from kafka import KafkaConsumer
import json


def deserializer(message):
    return json.loads(message.decode('utf-8'))


consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=deserializer
)


topics_list = ['first_topic', 'second_topic']
consumer.subscribe(topics_list)

while True:
    # poll messages each certain ms
    raw_messages = consumer.poll(
        timeout_ms=100, max_records=200
    )

    # for each messages batch
    for topic_partition, messages in raw_messages.items():
        for i in range(len(messages)):
            print(f"topic: {messages[i][0]}, messages: {messages[i][6]}")