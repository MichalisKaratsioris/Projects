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


"""
---------------------------- PART V -----------------------------
----------------------- MongoDB queries -------------------------
"""


def _starting():
    return "Start of the work-flow!"


def _mongo_aggr_01():
    client = MongoClient("mongodb+srv://MichalisKaratsioris:Password123@cluster.wpjmu9f.mongodb.net/?retryWrites=true&w=majority")
    db = client["GFA_Data_Engineering_Exam"]
    coll = db["airports"]
    # Which airports are located in Europe? Return only name and continent and sort the result by elevation_ft
    # in descending order.
    result = coll.find({{"continent": "Europe"}, {"name": 1, "continent": 1}, {"elevation_ft": -1}})
    return list(result)


def _mongo_aggr_02():
    client = MongoClient("mongodb+srv://MichalisKaratsioris:Password123@cluster.wpjmu9f.mongodb.net/?retryWrites=true&w=majority")
    db = client["GFA_Data_Engineering_Exam"]
    coll = db["airports"]
    # What is the count of airports that belongs to heliport airport type?
    result = coll.count({"type": "heliport"})
    return list(result)


def _ending():
    return "End of the work-flow!"


with DAG('GFA_Data_Engineering_Exam_dag05_MongoDB', default_args=default_args) as dag:
    starting_task = PythonOperator(
        task_id="start",
        python_callable=_starting
    )
    aggregation_01 = PythonOperator(
        task_id='mongo_aggregation_01',
        python_callable=_mongo_aggr_01
    )
    aggregation_02 = PythonOperator(
        task_id='mongo_aggregation_01',
        python_callable=_mongo_aggr_02
    )
    ending_task = PythonOperator(
        task_id="end",
        python_callable=_ending
    )

starting_task >> aggregation_01 >> aggregation_02 >> ending_task