from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dag_var = Variable.get("exercises_variables", deserialize_json=True)

def _variable():
    # return f"{dag_var}"
    return f"{dag_var['hello_text']}"

def _starting():
    return "Start of the work-flow!"

def _ending():
    return "End of the work-flow!"

with DAG("exercise02_dag", start_date=datetime(2022, 11, 5), schedule_interval='0 14 * * 1-5', catchup=False) as dag:
    

    starting_task = PythonOperator(
        task_id = "start",
        python_callable = _starting
    )

    trigger_target = TriggerDagRunOperator(
        task_id = 'exercise01_dag',
        trigger_dag_id = 'exercise01_dag'
    )

    variable_task = PythonOperator(
        task_id = "variable",
        python_callable = _variable
    )

    ending_task = PythonOperator(
        task_id = "end",
        python_callable = _ending
    )
    
    starting_task >> variable_task >> ending_task >> trigger_target