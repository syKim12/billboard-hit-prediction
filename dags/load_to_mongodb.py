import os, json, csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime,timedelta


def load_data():
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client['music']
    collection = db['chart']
    
    with open('billboard_2023.csv', 'r') as file:
        csv_data = csv.DictReader(file)
        for row in csv_data:
            collection.insert_one(row)

    client.close()


load_csv_mongo_dag =  DAG(
    dag_id="load-csv-to-mongo",
    schedule_interval=None,
    start_date=datetime(2023,7,29),
    catchup=False) 

load_data = PythonOperator(
    task_id='load-csv-to-mongo',
    python_callable=load_data,
    dag=load_csv_mongo_dag
    )
    
load_data