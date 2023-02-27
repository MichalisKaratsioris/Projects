# Install apache-airflow from "Python Packages"
from airflow import DAG
from datetime import datetime
# Install apachle-airflow-providers-microsoft-mssq from "Python Packages"
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# After installing the airflow.providers.microsoft, the airflow.operators.python will be installed automatically
from airflow.operators.python import PythonOperator
import pandas as pd

"""
additional packages (if you don't have it there by default, you need to put them in docker-compose.yml file like this:

_PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- apache-airflow-providers-microsoft-mssql[common.sql] 
pandas apache-airflow-providers-mongo}

"""


default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022, 11,15),
        'schedule_interval': '@weekly'
        }
# -----------------------------------------------------------------
# ----------------------------- MSSQL -----------------------------
# -----------------------------------------------------------------


# Extract
def _extract():
    hook = MsSqlHook(mssql_conn_id='airflow_mssql')
    sql = """
        SELECT *
        FROM etl.dbo.employees
        WHERE age>4
        """
    result = hook.get_records(sql)
    print(f'Data extraction complete: {result}')
    return result


# Transform
def _transform(ti):
    data = ti.xcom_pull(task_ids=['extract'])[0]
    df = pd.DataFrame(data, columns=['id', 'first_name', 'age'])
    # in the following line, I cast the values of "age" column into int, because during the table creation, the values
    # where declared as varchar(3)...
    df = df.loc[df['age'].astype(int) < 9]
    df['name_id'] = df['id'].astype(str) + " / " + df['first_name']
    result = df.values.tolist()
    print(f'Data extraction complete: {result}')
    return result


# Load
def _load(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    df = pd.DataFrame(data, columns=['id', 'name', 'age', 'name_id'])
    df = df[['name', 'age', 'name_id']]
    data = df.values.tolist()
    hook = MsSqlHook(mssql_conn_id='airflow_mssql')
    # remember to prepare a table inside a database - preferably the same - before going on to insert the transformed
    # data.
    hook.insert_rows('etl.dbo.employees_new', data)
    return 'Data loading compete.'


with DAG('MSSQL_connection', default_args=default_args) as dag:
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
