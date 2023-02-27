from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from random import uniform

def _starting():
    return "Start of the work-flow!"

def _ending():
    return "End of the work-flow!"

default_args = {
    'start_date': datetime(2022, 11, 4)
}

def _xcom_value_push(ti):
    ti.xcom_push(key='first_xcom', value='Hello XCom!')

def _xcom_value_pull(ti):
    key = 'first_xcom'
    xcom_value = ti.xcom_pull(key='first_xcom', task_ids='push_xcom_value')
    print(f"Key: {key}, Value: {xcom_value}")

with DAG('exercise03_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:


    starting_task = PythonOperator(
        task_id = "start",
        python_callable = _starting,
        do_xcom_push=False
    )
    
    push_xcom_value = PythonOperator(
        task_id = "push_xcom_value",
        python_callable = _xcom_value_push
    )

    pull_xcom_value = PythonOperator(
        task_id = "pull_xcom_value",
        python_callable = _xcom_value_pull
    )

    ending_task = PythonOperator(
        task_id = "end",
        python_callable = _ending,
        do_xcom_push=False
    )

    starting_task >> push_xcom_value >> pull_xcom_value >> ending_task