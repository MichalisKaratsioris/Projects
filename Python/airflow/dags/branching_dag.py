from airflow import DAG
from airflow.operators.empty import EmptyOperator as eo
from airflow.operators.python import BranchPythonOperator as bpo
from datetime import datetime
from random import randint

default_args = {
    'start_date': datetime(2022, 11, 4)
}

def _choose_best_model():
    accuracy = randint(1,10)
    if accuracy > 5:
        return 'accurate'
        # return ['accurate', 'inaccurate'] 
    return 'inaccurate'

with DAG('branching', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:


    choose_best_model = bpo(
        task_id='choose_best_model',
        python_callable=_choose_best_model
    )

    accurate = eo(
        task_id='accurate'
    )
    
    inaccurate = eo(
        task_id='inaccurate'
    )

    storing = eo(
        task_id='storing',
        trigger_rule='none_failed_or_skipped'
    )

    choose_best_model >> [accurate, inaccurate] >> storing