from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import app
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor


def _starting():
    return "Start of the work-flow!"


def _ending():
    return "End of the work-flow!"


def _extract_rest():

    return 5


def _extract_file():
    return 5


def _transform():
    return 1


def _load():
    return 1


with DAG("exercise08_dag", start_date=datetime(2022, 11, 5), schedule_interval='0 4 * * 1-5', catchup=False) as dag:
    # 1. Check if the API is up
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='api_products',
        endpoint='/api/products/'
    )

    starting_task = PythonOperator(
        task_id="start",
        python_callable=_starting
    )

    extract_rest = PythonOperator(
        task_id="extract_rest",
        python_callable=_extract_rest
    )

    extract_file = PythonOperator(
        task_id="extract_file",
        python_callable=_extract_file
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform
    )

    load = PythonOperator(
        task_id="load",
        python_callable=_load
    )

    ending_task = PythonOperator(
        task_id="end",
        python_callable=_ending
    )

    starting_task >> ending_task