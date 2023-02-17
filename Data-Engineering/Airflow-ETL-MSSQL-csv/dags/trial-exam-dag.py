from airflow import DAG
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from bson import ObjectId
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient


my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, 'dataset.csv')

default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022, 11, 18),
        'schedule_interval': '@weekly'
        }


# -----------------------------------------------------------------
# ----------------------------- MONGODB -----------------------------
# -----------------------------------------------------------------
load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

# Connection to the cluster
connection_str = f"mongodb+srv://MichalisKaratsioris:{password}@cluster.wpjmu9f.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_str)

dbs = client.list_database_names()
db_TrialExamDataEngineering = client.TrialExamDataEngineering
collection_Applicants = db_TrialExamDataEngineering.Applicants


# -----------------------------------------------------------------
# ----------------------------- MSSQL -----------------------------
# -----------------------------------------------------------------


# Extract
def _extract():
    applicants = []
    with open(configuration_file_path, 'r') as file:
        # insert the data in a list as lists
        for line in file:
            applicant = line.strip().split(',')
            applicants.append(applicant)
        applicants.remove(applicants[0])
        # remove duplicate rows
        for i in range(len(applicants) - 2):
            if applicants[i][0] == (applicants[i + 1][0]):
                applicants.remove(applicants[i + 1])
        # fill NaN values with "Unknown"
        for applicant in applicants:
            for e in applicant:
                if len(e) == 0:
                    applicant[applicant.index(e)] = 'Unknown'
        # Create new column "Class-name" and apply this logic:
        # If applicants preferred language is Java -> value of the column will be "Panthera", else -> "Celadon".
        # for applicant in applicants:
        #     if applicant[-2] == 'Java':
        #         applicant.append('1')
        #     else:
        #         applicant.append('2')
    return applicants


# Transform
def _transform(ti):
    data = ti.xcom_pull(task_ids=['extract'])[0]
    df = pd.DataFrame(data, columns=['name_surname', 'age', 'address', 'preferred_language', 'cognitive_score'])
    # Filter out these rows, where applicants cognitive score is below 50
    df = df.loc[df['cognitive_score'].astype(int) >= 50]
    result = df.values.tolist()
    return result


# Load
def _load(ti):
    data = ti.xcom_pull(task_ids=['transform'])[0]
    df = pd.DataFrame(data, columns=['name_surname', 'age', 'address', 'preferred_language', 'cognitive_score'])
    df = df[['name_surname', 'age', 'address', 'preferred_language', 'cognitive_score']]
    # df["class_id"] = df["class_id"].astype(int)
    data = df.values.tolist()
    hook = MsSqlHook(mssql_conn_id='airflow_mssql')
    hook.insert_rows('trialExamDataEngineer.dbo.applicants', data)
    return 'Data loading compete.'


with DAG('Trial_Exam_Data_Engineering', default_args=default_args) as dag:
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
