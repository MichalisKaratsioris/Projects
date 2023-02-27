from airflow import DAG
import json
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, "test.json")

def _starting():
    return "Start of the work-flow!"

def _json_read():
    with open(configuration_file_path, 'r') as f:
        file = json.load(f)
    return file

def _ending():
    return "End of the work-flow!"

with DAG("exercise07_dag", start_date=datetime(2022, 11, 5), schedule_interval='0 14 * * 1-5', catchup=False) as dag:
    
    starting_task = PythonOperator(
        task_id = "start",
        python_callable = _starting
    )

    json_read = PythonOperator(
        task_id = "json_read",
        python_callable = _json_read
    )

    ending_task = PythonOperator(
        task_id = "end",
        python_callable = _ending
    )
    
    starting_task >> json_read >> ending_task