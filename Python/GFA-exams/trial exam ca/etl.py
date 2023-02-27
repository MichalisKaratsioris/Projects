from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import os

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from pymongo import MongoClient


dag_dir = os.path.dirname(os.path.abspath(__file__))
csv_file = os.path.join(dag_dir, "dataset.csv")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 22),
    'schedule_interval': '@weekly'
}

dag = DAG('our_new_ETL', default_args=default_args)

"""
SQL QUERY to create database with foreign keys (can be done directly in Azure) - table initialization

CREATE DATABASE etl;

USE etl;

CREATE TABLE classes (
    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO classes 
VALUES ('Panthera'), ('Celadon');


CREATE TABLE applicants (
    id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    name_and_surname VARCHAR(100),
    age INT,
    address VARCHAR(100),
    preferred_language VARCHAR(100),
    cognitive_score VARCHAR(100),
    class_id INT

    CONSTRAINT FK_applicants_classes FOREIGN KEY (class_id)
        REFERENCES etl.dbo.classes (id)
);

"""


def _extract_transform_csv():
    df = pd.read_csv(csv_file)
    df.drop_duplicates(inplace=True)
    df.fillna("Unknown", inplace=True)
    df = df.loc[df["Cognitive-score"]>=50]
    df.loc[df["Preffered-language"] == "Java", "Class-name"] = 1
    df.loc[df["Preffered-language"] != "Java", "Class-name"] = 2
    return df.values.tolist()
    

def _load(ti):
    data = ti.xcom_pull(task_ids=['extract_transform_csv'])[0]
    df = pd.DataFrame(data, columns=["name_and_surname", "age", "address", "preferred_language", "cognitive_score", "class_id"])
    df["class_id"] = df["class_id"].astype(int)
    result = df.values.tolist()
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    hook.insert_rows("etl.dbo.applicants", result)
    return 0

def _load_to_mongo():
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    sql = """
        SELECT
        a.name_and_surname, a.age, a.cognitive_score,
        c.name
        FROM etl.dbo.applicants a
        LEFT JOIN etl.dbo.classes c
        ON a.class_id=c.id;
    """
    df = hook.get_pandas_df(sql)
    client = MongoClient("your-mongo-url")
    db = client["exam"]
    coll = db["Applicants"]
    coll.insert_many(df.to_dict('records'))
    return 0


extract_transform_csv = PythonOperator(task_id="extract_transform_csv", python_callable=_extract_transform_csv, dag=dag)
load_to_mssql = PythonOperator(task_id="load_to_mssql", python_callable=_load, dag=dag)
load_to_mongo = PythonOperator(task_id="load_to_mongo", python_callable=_load_to_mongo, dag=dag)

extract_transform_csv >> load_to_mssql >> load_to_mongo