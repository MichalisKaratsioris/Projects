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
---------------------------- PART IV -----------------------------
------------------------- MSSQL queries --------------------------
"""


def _starting():
    return "Start of the work-flow!"


def _aggregation_01_pandas_print():
    hook = MsSqlHook(mssql_conn_id='airflow-mssql')
    # What are the top 10 highest airports based on the elevation_ft (with ties)?
    sql = """
        SELECT TOP(10) WITH TIES *
        FROM airports
        ORDER BY elevation_ft
        """
    df = hook.get_pandas_df(sql)
    print(df.head())


def _aggregation_02_pandas_print():
    hook = MsSqlHook(mssql_conn_id='airflow-mssql')
    # Which airports are located in Africa?
    sql = """
        SELECT *
        FROM countries c
        WHERE c.continent LIKE 'Africa'
        JOIN airports a ON a.iso_country = c.iso2_code
        """
    df = hook.get_pandas_df(sql)
    print(df.head())


def _aggregation_03_pandas_print():
    hook = MsSqlHook(mssql_conn_id='airflow-mssql')
    # Which airports local code start with 02? Return only name and local_code.
    sql = """
        SELECT name, local_code
        FROM airports
        WHERE local_code LIKE '02%'
        """
    df = hook.get_pandas_df(sql)
    print(df.head())


def _aggregation_04_pandas_print():
    hook = MsSqlHook(mssql_conn_id='airflow-mssql')
    # What are the highest elevation_ft by continents?
    sql = """
        WITH
        continent_elevation AS (
            SELECT c.continent AS continent, a.elevation_ft as elevation
            FROM airports a
            JOIN countries c ON a.iso_country = c.iso2_code
            ORDER BY a.elevation_ft DESC
        ),
        SELECT TOP (1) *
        FROM continent_elevation
        GROUP BY continent
        """
    df = hook.get_pandas_df(sql)
    print(df.head())


def _aggregation_05_pandas_print():
    hook = MsSqlHook(mssql_conn_id='airflow-mssql')
    # What is the count of airports that belongs to small_airport airport type?
    sql = """
        SELECT COUNT(type)
        FROM airports
        WHERE type LIKE 'small_airport'
        """
    df = hook.get_pandas_df(sql)
    print(df.head())


def _ending():
    return "End of the work-flow!"


with DAG('GFA_Data_Engineering_Exam_dag04_MSSQL', default_args=default_args) as dag:
    starting_task = PythonOperator(
        task_id="start",
        python_callable=_starting
    )
    aggregation_01 = PythonOperator(
        task_id='mssql_aggregation_01',
        python_callable=_aggregation_01_pandas_print
    )
    aggregation_02 = PythonOperator(
        task_id='mssql_aggregation_02',
        python_callable=_aggregation_02_pandas_print
    )
    aggregation_03 = PythonOperator(
        task_id='mssql_aggregation_03',
        python_callable=_aggregation_03_pandas_print
    )
    aggregation_04 = PythonOperator(
        task_id='mssql_aggregation_04',
        python_callable=_aggregation_04_pandas_print
    )
    aggregation_05 = PythonOperator(
        task_id='mssql_aggregation_05',
        python_callable=_aggregation_05_pandas_print
    )
    ending_task = PythonOperator(
        task_id="end",
        python_callable=_ending
    )

starting_task >> aggregation_01 >> aggregation_02 >> aggregation_03 >> aggregation_04 >> aggregation_05 >> ending_task
