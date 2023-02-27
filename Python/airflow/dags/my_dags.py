from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator as po
from airflow.operators.python import BranchPythonOperator as bpo
from airflow.operators.bash import BashOperator as bo
from random import randint

def _training_model():
    return randint(1, 10)

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=["training_model_A", "training_model_B", "training_model_C"])
    best_accuracy = max(accuracies)
    if (best_accuracy >= 8):
        return 'accurate'
    return 'inaccurate'

# The triggering of this DAG will take place after on start_date + schedule_interval. In our example this is
# on midnight of November 5th, 2022.
with DAG("my_dag", start_date=datetime(2022, 11, 4), schedule_interval="@daily", catchup=False) as dag:
    
    training_model_A = po(
        task_id="training_model_A",
        python_callable=_training_model
    )

    
    training_model_B = po(
        task_id="training_model_B",
        python_callable=_training_model
    )

    
    training_model_C = po(
        task_id="training_model_C",
        python_callable=_training_model
    )

    choose_best_model = bpo(
        task_id="choose_best_model",
        python_callable=_choose_best_model
    )

    accurate = bo(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )
    
    inaccurate = bo(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

    [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]

