from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import os

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd


default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022, 11,20),
        'schedule_interval': '@weekly'
        }

dag = DAG('ETL_SQL_aggr', default_args=default_args)


# 3 variations for using the Hook (can be applied not just for the first aggregation task)

def _aggregation_01():
    hook = MsSqlHook(mssql_conn_id='mssql_default')
    sql = """
        SELECT 
        preferred_language,
        AVG(cognitive_score) as avg_score,
        MAX(cognitive_score) as max_score,
        MIN(cognitive_score) as min_score
        FROM etl.dbo.applicants
        GROUP BY preferred_language;
        """
    return hook.get_records(sql)

def _aggregation_01_to_pandas():
    hook = MsSqlHook(mssql_conn_id='mssql_default')
    sql = """
        SELECT 
        preferred_language,
        AVG(cognitive_score) as avg_score,
        MAX(cognitive_score) as max_score,
        MIN(cognitive_score) as min_score
        FROM etl.dbo.applicants
        GROUP BY preferred_language;
        """
    df = hook.get_pandas_df(sql)
    print(df.head())
    
def _aggregation_01_select_into_new_table():
    hook = MsSqlHook(mssql_conn_id='mssql_default')
    sql = """
        SELECT 
        preferred_language,
        AVG(cognitive_score) as avg_score,
        MAX(cognitive_score) as max_score,
        MIN(cognitive_score) as min_score
        INTO etl.dbo.aggregation_01
        FROM etl.dbo.applicants
        GROUP BY preferred_language;
        """
    hook.run(sql)
    return 0
    
    
def _aggregation_02():
    hook = MsSqlHook(mssql_conn_id='mssql_default')
    sql = """
        SELECT
        name_and_surname
        FROM etl.dbo.applicants
        WHERE age>=35 AND age<=65 AND cognitive_score>(
        SELECT AVG(cognitive_score) as avg_score FROM etl.dbo.applicants
        );
        """
    return hook.get_records(sql)

def _aggregation_03():
    hook = MsSqlHook(mssql_conn_id='mssql_default')
    sql = """
        WITH 
        java_top3 AS (
                SELECT TOP (3)
                preferred_language, name_and_surname, cognitive_score
                FROM etl.dbo.applicants
                WHERE preferred_language LIKE 'Java'
                ORDER BY cognitive_score DESC
        ),
        python_top3 AS (
                SELECT TOP (3)
                preferred_language, name_and_surname, cognitive_score
                FROM etl.dbo.applicants
                WHERE preferred_language LIKE 'Python'
                ORDER BY cognitive_score DESC
        ),
        js_top3 AS (
                SELECT TOP (3)
                preferred_language, name_and_surname, cognitive_score
                FROM etl.dbo.applicants
                WHERE preferred_language LIKE 'Javascript'
                ORDER BY cognitive_score DESC
        )
        SELECT
        *
        FROM java_top3
        UNION ALL
        SELECT
        *
        FROM python_top3
        UNION ALL
        SELECT
        *
        FROM js_top3;
        """
    return hook.get_records(sql)
  
aggregation_01 = PythonOperator(task_id='aggregation_01', python_callable=_aggregation_01, dag=dag)
aggregation_01_select_into_new_table = PythonOperator(task_id='aggregation_01_select_into_new_table', python_callable=_aggregation_01_select_into_new_table, dag=dag)
aggregation_01_var = PythonOperator(task_id='aggregation_01_var', python_callable=_aggregation_01_to_pandas, dag=dag)
aggregation_02 = PythonOperator(task_id='aggregation_02', python_callable=_aggregation_02, dag=dag)
aggregation_03 = PythonOperator(task_id='aggregation_03', python_callable=_aggregation_03, dag=dag)

[aggregation_01, aggregation_01_var, aggregation_01_select_into_new_table] >> aggregation_02 >> aggregation_03