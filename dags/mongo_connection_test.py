import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime,timedelta


def test_mongo():
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client.music
    #currency_collection=db.currency_collection
    print(f"Connected to MongoDB - {client.server_info()}")


mongo_dag =  DAG(
    dag_id="test_mongodb_conn",
    schedule_interval=None,
    start_date=datetime(2023,7,29),
    catchup=False) 

t1 = PythonOperator(
    task_id='test-mongodb-connection',
    python_callable=test_mongo,
    dag=mongo_dag
    )
    
t1