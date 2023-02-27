# from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os
from kafka import KafkaProducer
import json
from time import sleep


def _starting():
    return "Start of the work-flow!"


def _ending():
    return "End of the work-flow!"


# ----------------------------------------------------------------------------------
# ---------------------------------- Extract Sports --------------------------------
# ----------------------------------------------------------------------------------
def _extract_sports():
    my_dir = os.path.dirname(os.path.abspath(__file__))
    configuration_file_path = os.path.join(my_dir, 'sports.json')
    url = 'https://odds.p.rapidapi.com/v4/sports'
    querystring = {"all": "true"}
    headers = {
        "X-RapidAPI-Key": "ea61df56a8msh21c6250be4fb258p189a1djsn715926ac5b42",
        "X-RapidAPI-Host": "odds.p.rapidapi.com"
    }
    query = requests.request("GET", url, headers=headers, params=querystring)
    query_list = query.text
    if len(query_list) > 1:
        query_list = query_list.split('},')
    query_list[0] = query_list[0].strip('[')
    query_list[-1] = query_list[-1].strip(']')
    for i in range(len(query_list) - 1):
        query_list[i] = json.loads(query_list[i] + '}')
    sports_json_object = json.dumps(query_list, indent=4)
    with open(configuration_file_path, "w") as f:
        f.write(sports_json_object)
    if sports_json_object:
        return 'Data transfer success!'
    return 'Data transfer failure...'


# ----------------------------------------------------------------------------------
# ------------------------------ Extract Scores ------------------------------------
# ----------------------------------------------------------------------------------
def _extract_scores():
    my_dir = os.path.dirname(os.path.abspath(__file__))
    configuration_file_path = os.path.join(my_dir, 'scores.json')
    url = "https://odds.p.rapidapi.com/v4/sports/americanfootball_ncaaf/scores"
    querystring = {"daysFrom": "3"}
    headers = {
        "X-RapidAPI-Key": "ea61df56a8msh21c6250be4fb258p189a1djsn715926ac5b42",
        "X-RapidAPI-Host": "odds.p.rapidapi.com"
    }
    query = requests.request("GET", url, headers=headers, params=querystring)
    query_list = query.text
    if len(query_list) > 1:
        query_list = query_list.split('},')
    query_list[0] = query_list[0].strip('[')
    query_list[-1] = query_list[-1].strip(']')
    for i in range(len(query_list) - 1):
        query_list[i] = json.loads(query_list[i])
    scores_json_object = json.dumps(query_list, indent=4)
    with open(configuration_file_path, "w") as f:
        f.write(scores_json_object)
    if scores_json_object:
        return 'Data transfer success!'
    return 'Data transfer failure...'


# ----------------------------------------------------------------------------------
# ------------------------------ Extract Odds --------------------------------------
# ----------------------------------------------------------------------------------
def _extract_odds():
    my_dir = os.path.dirname(os.path.abspath(__file__))
    configuration_file_path = os.path.join(my_dir, 'odds.json')
    url = "https://odds.p.rapidapi.com/v4/sports/americanfootball_ncaaf/odds"
    querystring = {"regions": "us", "oddsFormat": "decimal", "markets": "h2h,spreads", "dateFormat": "iso"}
    headers = {
        "X-RapidAPI-Key": "ea61df56a8msh21c6250be4fb258p189a1djsn715926ac5b42",
        "X-RapidAPI-Host": "odds.p.rapidapi.com"
    }
    query = requests.request("GET", url, headers=headers, params=querystring)
    query_list = query.text
    if len(query_list) > 1:
        query_list = query_list.split('},')
    query_list[0] = query_list[0].strip('[')
    query_list[-1] = query_list[-1].strip(']')
    for i in range(len(query_list) - 1):
        query_list[i] = json.loads(query_list[i] + '}')
    odds_json_object = json.dumps(query_list, indent=4)
    with open(configuration_file_path, "w") as f:
        f.write(odds_json_object)
    if odds_json_object:
        return 'Data transfer success!'
    return 'Data transfer failure...'


# ----------------------------------------------------------------------------------
# ------------------------------- Define Kafka Producer -----------------------------
# ----------------------------------------------------------------------------------
def serializer(message):
    return json.dumps(message, sort_keys=True, indent=4).encode('utf-8')


producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=serializer
    )


def _produce_sports():
    my_dir = os.path.dirname(os.path.abspath(__file__))
    configuration_file_path = os.path.join(my_dir, 'scores.json')
    url = "https://odds.p.rapidapi.com/v4/sports/americanfootball_ncaaf/scores"
    querystring = {"daysFrom": "3"}
    headers = {
        "X-RapidAPI-Key": "ea61df56a8msh21c6250be4fb258p189a1djsn715926ac5b42",
        "X-RapidAPI-Host": "odds.p.rapidapi.com"
    }
    query = requests.request("GET", url, headers=headers, params=querystring)
    if query.text.__eq__('[]'):
        return 'No new data available.'
    scores_json_object = json.dumps(query.text, indent=4)
    with open(configuration_file_path, "w") as f:
        f.write(scores_json_object)
    topic = 'scores_api'
    producer.send(topic, value='1')
    sleep(1)
    if scores_json_object:
        return 'Success! Data transferred to Kafka and .json file created.'
    return 'Data transfer failure...'


# ----------------------------------------------------------------------------------
# ------------------------------- Unleash the KRAKEN -------------------------------
# ----------------------------------------------------------------------------------
with DAG("elt_pipeline", start_date=datetime(2022, 11, 5), schedule_interval='0 4 * * 1-5', catchup=False) as dag:
    starting_task = PythonOperator(
        task_id="start",
        python_callable=_starting
    )

    extract_sports = PythonOperator(
        task_id="extract_sports",
        python_callable=_extract_sports
    )

    extract_scores = PythonOperator(
        task_id="extract_scores",
        python_callable=_extract_scores
    )

    extract_odds = PythonOperator(
        task_id="extract_odds",
        python_callable=_extract_odds
    )

    produce_sports = PythonOperator(
        task_id="produce_scores",
        python_callable=_produce_sports
    )

    ending_task = PythonOperator(
        task_id="end",
        python_callable=_ending
    )

    # starting_task >> ending_task
    starting_task >> extract_sports >> produce_sports >> ending_task
    # starting_task >> extract_sports >> extract_scores >> extract_odds >> ending_task
    # starting_task >> extract_sports >> ending_task
