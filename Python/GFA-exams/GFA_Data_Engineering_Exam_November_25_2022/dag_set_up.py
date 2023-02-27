from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from pymongo import MongoClient

dag_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(dag_dir, "")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 22),
    'schedule_interval': '@weekly'
}


# Extract Function
def _extract():
    pass


# Transform Function
def _transform(ti):
    pass


# Load Function
def _load(ti):
    pass


with DAG('GFA_Data_Engineering_Exam_dag##', default_args=default_args) as dag:
    extract = PythonOperator(
        task_id='extract',
        python_callable=_extract
    )
    transform = PythonOperator(
        task_id='transform',
        python_callable=_transform
    )
    load = PythonOperator(
        task_id='load',
        python_callable=_load
    )

extract >> transform >> load

# os.remove("demofile.txt")
"""
---------------------------- PART I -----------------------------
----------------------------- MSSQL -----------------------------
---------------------------- MONGODB ----------------------------
"""