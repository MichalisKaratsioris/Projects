from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def _greeting():
    return "Hello World!"

def _starting():
    return "Start of the work-flow!"

def _ending():
    return "End of the work-flow!"

with DAG("exercise01_dag", start_date=datetime(2022, 11, 5), schedule_interval='0 14 * * 1-5', catchup=False) as dag:
    
    starting_task = PythonOperator(
        task_id = "start",
        python_callable = _starting
    )

    greeting_task = PythonOperator(
        task_id = "greeting",
        python_callable = _greeting
    )

    ending_task = PythonOperator(
        task_id = "end",
        python_callable = _ending
    )
    
    starting_task >> greeting_task >> ending_task