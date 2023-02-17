from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 22),
    'schedule_interval': '@weekly'
}


"""
---------------------------- PART II -----------------------------
------------------------ MSSQL pipeline --------------------------
------------------------ .csv Airports ---------------------------
"""


def _starting():
    return "Start of the work-flow!"


# Extract And Transform Function
def _extract_transform_csv():
    my_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(my_dir, 'airports.csv')
    df = pd.read_csv(csv_file)
    df = df[df['gps_code'].notna()]
    df['elevation_ft'].fillna(0, inplace=True)
    return df.values.tolist()


# Load Function
def _load(ti):
    data = ti.xcom_pull(task_ids=['extract_transform_csv'])[0]
    df = pd.DataFrame(data, columns=["ident", "type", "name", "elevation_ft ", "continent", "iso_country", "iso_region", "municipality", "gps_code", "iata_code", "local_code", "coordinates"])
    df["elevation_ft"] = df["elevation_ft"].astype(int)
    df = df.drop('continent', axis=1)
    result = df.values.tolist()
    hook = MsSqlHook(mssql_conn_id="airflow-mssql")
    hook.insert_rows("GFA_Data_Engineering_Exam.dbo.airports", result)
    return 0


def _ending():
    return "End of the work-flow!"


with DAG('GFA_Data_Engineering_Exam_dag02_ETL_csv2MSSQL', default_args=default_args) as dag:
    starting_task = PythonOperator(
        task_id="start",
        python_callable=_starting
    )
    extract_transform_csv = PythonOperator(
        task_id='extract_transform_csv',
        python_callable=_extract_transform_csv
    )
    load = PythonOperator(
        task_id='load',
        python_callable=_load
    )
    ending_task = PythonOperator(
        task_id="end",
        python_callable=_ending
    )

starting_task >> extract_transform_csv >> load >> ending_task
