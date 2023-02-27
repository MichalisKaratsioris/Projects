from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2022, 11, 4)
}

def _print():
    print("Another DAG was triggered!")

def _downloading():
    print('downloading')

with DAG('exercise05_dag', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    trigger_dag = TriggerDagRunOperator(
        task_id = 'trigger_dag',
        trigger_dag_id = 'exercise04_dag',
        # wait_for_completion=False
        wait_for_completion=True
    )

    print_info = PythonOperator (
        task_id = 'print_info',
        python_callable=_print
    )

    trigger_dag >> print_info