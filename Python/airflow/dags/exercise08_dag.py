from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def _starting():
    return "Start of the work-flow!"

def _ending():
    return "End of the work-flow!"

data_proc_script = r"""USE dags;

SELECT TOP (1) * FROM exercise08;
"""

with DAG("exercise08_dag", start_date=datetime(2022, 11, 5), schedule_interval='0 14 * * 1-5', catchup=False) as dag:

    starting_task = PythonOperator(
        task_id = "start",
        python_callable = _starting
    )

    mssql_select_task = MsSqlOperator(
        task_id='mssql_select_task',
        mssql_conn_id='airflow_mssql',
        sql=data_proc_script
    )

    ending_task = PythonOperator(
        task_id = "end",
        python_callable = _ending
    )
    
    starting_task >> mssql_select_task >> ending_task