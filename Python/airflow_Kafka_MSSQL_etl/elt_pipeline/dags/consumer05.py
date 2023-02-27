import os

from kafka import KafkaConsumer
import json


def deserializer(message):
    return json.loads(message.decode('utf-8'))


consumer_sports = KafkaConsumer(
    'sports_api',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=deserializer
)

consumer_scores = KafkaConsumer(
    'scores_api',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=deserializer
)

consumer_odds = KafkaConsumer(
    'odds_api',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=deserializer
)


while True:
    for msg in consumer_sports:
        # if msg.value.__eq__('[]'):
        #     continue
        sports_json_object = json.dumps(msg.value, indent=4)
        my_dir = os.path.dirname(os.path.abspath(__file__))
        configuration_file_path = os.path.join(my_dir, 'sports_sql.json')
        with open(configuration_file_path, "w") as f:
            f.write(sports_json_object)
        for msg in consumer_scores:
            # if msg.value.__eq__('[]'):
            #     continue
            scores_json_object = json.dumps(msg.value, indent=4)
            my_dir = os.path.dirname(os.path.abspath(__file__))
            configuration_file_path = os.path.join(my_dir, 'scores_sql.json')
            with open(configuration_file_path, "w") as f:
                f.write(scores_json_object)
            for msg in consumer_odds:
                # if msg.value.__eq__('[]'):
                #     continue
                odds_json_object = json.dumps(msg.value, indent=4)
                my_dir = os.path.dirname(os.path.abspath(__file__))
                configuration_file_path = os.path.join(my_dir, 'odds_sql.json')
                with open(configuration_file_path, "w") as f:
                    f.write(odds_json_object)

