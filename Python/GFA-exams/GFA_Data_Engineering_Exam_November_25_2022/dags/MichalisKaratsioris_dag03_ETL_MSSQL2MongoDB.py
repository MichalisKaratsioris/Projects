from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from pymongo import MongoClient


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 22),
    'schedule_interval': '@weekly'
}


"""
---------------------------- PART III -----------------------------
------------------------ MongoDB pipeline -------------------------
"""


def _starting():
    return "Start of the work-flow!"


# Extract And Transform Function
def _extract_transform_mssql():
    my_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(my_dir, 'airports.csv')
    df = pd.read_csv(csv_file)
    df = df[df['gps_code'].notna()]
    df['elevation_ft'].fillna(0, inplace=True)
    return df.values.tolist()


# Load Function
def _load(ti):
    hook = MsSqlHook(mssql_conn_id="airflow-mssql")
    sql = """
        SELECT
        a.name, a.type, a.elevation_ft, a.iso_region, a.gps_code, a.local_code, c.name, c.continent
        FROM GFA_Data_Engineering_Exam.dbo.countries c
        LEFT JOIN GFA_Data_Engineering_Exam.dbo.airports a
        ON a.iso_country = c.iso2_code;
    """
    df = hook.get_pandas_df(sql)
    client = MongoClient("mongodb+srv://MichalisKaratsioris:Password123@cluster.wpjmu9f.mongodb.net/?retryWrites=true&w=majority")
    db = client["GFA_Data_Engineering_Exam"]
    coll = db["airports"]
    coll.insert_many(df.to_dict('records'))
    return 0


def _ending():
    return "End of the work-flow!"


with DAG('GFA_Data_Engineering_Exam_dag03_ETL_MSSQL2MongoDB', default_args=default_args) as dag:
    starting_task = PythonOperator(
        task_id="start",
        python_callable=_starting
    )
    extract_transform_mssql = PythonOperator(
        task_id='extract_transform_mssql',
        python_callable=_extract_transform_mssql
    )
    load = PythonOperator(
        task_id='load',
        python_callable=_load
    )
    ending_task = PythonOperator(
        task_id="end",
        python_callable=_ending
    )

starting_task >> extract_transform_mssql >> load >> ending_task