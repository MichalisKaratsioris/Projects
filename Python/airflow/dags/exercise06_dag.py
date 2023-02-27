from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun

from datetime import datetime

default_args = {
    'start_date': datetime(2022, 11, 4)
}

def _dag_run_data():
    dag_id = 'my_dag'
    dagbag = DagBag()
    if dag_id not in dagbag.dags:
        error_message = f"Dag_id: {dag_id} not found."
        return error_message
    dag_runs = list()
    for run in DagRun.find(dag_id=dag_id):
        dag_runs.append(
            {
                # 'id': run.id,
                # 'run_id': run.run_id,
                # 'state': run.state,
                'dag_id': run.dag_id,
                'execution_date': run.execution_date.isoformat(),
                # 'start_date': ((run.start_date or '') and run.start_date.isoformat())
                
            }
        )
    result = [
        {
            "first_time": dag_runs[0]
        },
        {
            "last_time": dag_runs[-1]
        }
    ]
    return result

with DAG('exercise06_dag', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    dag_run_data = PythonOperator(
        task_id = "dag_run_data",
        python_callable=_dag_run_data
    )