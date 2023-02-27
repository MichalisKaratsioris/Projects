from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import os
import pandas as pd
from pymongo import MongoClient


default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022, 11,20),
        'schedule_interval': '@weekly'
        }

dag = DAG('ETL_mongo_select', default_args=default_args)

def _mongo_aggr_01():
    client = MongoClient("your-mongo-url")
    db = client["trialexam"]
    coll = db["applicants"]
    result = coll.find({"cognitive_score": {"$gt": 80}}, {"name_and_surname": 1, "cognitive_score": 1, "_id": 0})
    return list(result)

def _mongo_aggr_02():
    client = MongoClient("your-mongo-url")
    db = client["trialexam"]
    coll = db["applicants"]
    result = coll.find_one(sort=[("age", -1)])
    return list(result)

  
mongo_aggr_01 = PythonOperator(task_id='mongo_aggr_01', python_callable=_mongo_aggr_01, dag=dag)
mongo_aggr_02 = PythonOperator(task_id='mongo_aggr_02', python_callable=_mongo_aggr_02, dag=dag)

mongo_aggr_01 >> mongo_aggr_02