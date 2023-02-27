# Install apache-airflow from "Python Packages"
from airflow import DAG
from datetime import datetime
from pymongo import MongoClient
from airflow.providers.mongo.hooks.mongo import MongoHook
# After installing the airflow.providers.microsoft, the airflow.operators.python will be installed automatically
from airflow.operators.python import PythonOperator
import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv, find_dotenv
import os
import pprint



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
# ----------------------------- MongoDB -----------------------------
# -----------------------------------------------------------------
load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")
connection_str = f"mongodb+srv://MichalisKaratsioris:{password}@cluster.wpjmu9f.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_str)
try:
        dbs = client.list_database_names()
        print("--------- dbs ---------", dbs)
        mongoProject = client.mongoProject
        production = client.production
        collections_mongoProject = mongoProject.list_collection_names()
        collections_production = production.list_collection_names()
        print("--------- mongoProject collection ---------", collections_mongoProject)
        print("--------- production ---------", collections_production)
except Exception as e:
    print(e, "Unable to connect to the server.")