from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime
from random import randint

default_args = {
    'start_date': datetime(2022, 11, 4)
}

def _starting():
    return "Start of the work-flow!"

def _ending():
    return "End of the work-flow!"

def _odd():
    print("odd")

def _even():
    print("even")

def _xcom_pull(ti):
    value = ti.xcom_pull(key='branch_num', task_ids='push_xcom_task')
    if value % 2 == 0:
        return 'even'
    return 'odd'

def _xcom_push(ti):
    value = randint(1, 100)
    ti.xcom_push(key='branch_num', value=value)

with DAG('exercise04_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    starting_task = PythonOperator(
        task_id = 'start',
        python_callable = _starting,
        do_xcom_push=False
    )

    push_xcom_task = PythonOperator(
        task_id = 'push_xcom_task',
        python_callable = _xcom_push
    )

    odd = PythonOperator(
        task_id = 'odd',
        python_callable = _odd
    )


    even = PythonOperator(
        task_id = 'even',
        python_callable = _even
    )

    branch_num_task =BranchPythonOperator(
        task_id = 'branch_num_task',
        python_callable = _xcom_pull
    )

    ending_task = PythonOperator(
        task_id = 'end',
        python_callable = _ending,
        do_xcom_push=False,
        trigger_rule='none_failed_or_skipped'
    )
    
    starting_task >> push_xcom_task >> branch_num_task >> [odd, even] >> ending_task