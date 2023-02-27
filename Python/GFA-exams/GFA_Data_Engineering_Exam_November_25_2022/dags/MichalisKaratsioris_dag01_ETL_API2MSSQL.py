import json
import requests
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 22),
    'schedule_interval': '@weekly'
}


"""
---------------------------- PART I -----------------------------
------------------------ MSSQL pipeline -------------------------
------------------------ API Countries --------------------------
"""


def _starting():
    return "Start of the work-flow!"


# Extract And Transform Function
def _extract_transform_api():
    url = 'https://restcountries.com/v3.1/all'
    query = requests.request("GET", url)
    query_list = query.text
    json_object = json.loads(query_list)
    data = []
    for i in range(len(json_object)):
        data.append([json_object[i]['name']['common'], json_object[i]['cca2'], json_object[i]['continents'][0]])
    return data


# Load Function
def _load(ti):
    data = ti.xcom_pull(task_ids=['extract_transform_api'])[0]
    df = pd.DataFrame(data, columns=["name", "iso2_code", "continent"])
    result = df.values.tolist()
    hook = MsSqlHook(mssql_conn_id="airflow-mssql")
    hook.insert_rows("GFA_Data_Engineering_Exam.dbo.countries", result)
    return "The data were loaded to SQL Server table: 'GFA_Data_Engineering_Exam.dbo.countries' successfully."


def _ending():
    return "End of the work-flow!"


with DAG('GFA_Data_Engineering_Exam_dag01_ETL_API2MSSQL', default_args=default_args) as dag:
    starting_task = PythonOperator(
        task_id="start",
        python_callable=_starting
    )
    extract_transform_api = PythonOperator(
        task_id='extract_transform_api',
        python_callable=_extract_transform_api
    )
    load = PythonOperator(
        task_id='load',
        python_callable=_load
    )
    ending_task = PythonOperator(
        task_id="end",
        python_callable=_ending
    )

starting_task >> extract_transform_api >> load >> ending_task
