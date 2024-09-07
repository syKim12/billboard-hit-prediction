import os, json, csv, spotipy, time, torch
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime,timedelta
from spotipy.oauth2 import SpotifyClientCredentials
import mlflow
from argparse import ArgumentParser
import pandas as pd
import pymongo
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from airflow.hooks.base_hook import BaseHook
from be_great import GReaT

def save_model_to_registry():
    # 0. set mlflow environments
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://mlflow-artifact-store:9000"
    os.environ["MLFLOW_TRACKING_URI"] = "http://mlflow-server:5000"
    os.environ["AWS_ACCESS_KEY_ID"] = "minio"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "miniostorage"

    connection = BaseHook.get_connection("mlflow_default")
    print("Current MLflow Tracking URI:", mlflow.get_tracking_uri())

    secrets = json.loads(open('/opt/airflow/dags/secrets.json').read())

    # 1. get data
    conn = pymongo.MongoClient(host=secrets["mongo_host"], 
                            port=27017, 
                            username=secrets["mongo_user"], 
                            password=secrets["mongo_pw"])
    db_name = "music"
    db = conn.get_database(db_name)
    chart = db.get_collection("chart")
    non_chart = db.get_collection("non_chart")
    chart_df = pd.DataFrame(list(chart.find()))
    # chart_df = chart_df[chart_df["Date"].isna() == False]
    non_chart_df = pd.DataFrame(list(non_chart.find()))
    columns = ["Followers", "Acousticness", "Danceability", "Duration_ms", "Energy", 
            "Instrumentalness", "Liveness", "Loudness", "Speechiness", "Tempo", "Valence", "Hit"]
    chart_df = chart_df[columns].dropna()
    chart_df = chart_df.drop_duplicates()
    non_chart_df = non_chart_df[columns]
    df = pd.concat([chart_df, non_chart_df], axis=0)
    df["Hit"] = df["Hit"].astype("int")

    # ++Synthesize non-chart data
    n_samples = len(df[df["Hit"] == 1]) - len(df[df["Hit"] == 0])
    print('samples: ', n_samples)
    great = GReaT.load_from_dir("/opt/airflow/GreaT")
    print('model loaded!')
    syn_non_chart = great.sample(n_samples=n_samples, device="cpu")

    df = pd.concat([df, syn_non_chart], axis=0)
    for c in columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    X = df.iloc[:, (df.columns != "Hit")]
    X = X.astype(float)
    y = df.iloc[:, -1]
    y = y.astype("int")

    X_train, X_valid, y_train, y_valid = train_test_split(X, y, train_size=0.7, random_state=2024)



    # 2. model development and train
    parameters={'max_depth':[1,3,5,10],
                'n_estimators':[100,200,300],
                'learning_rate':[1e-3,0.01,0.1,1],
                'random_state':[1]}

    model_pipeline = Pipeline([("scaler", StandardScaler()), 
                            ("classifier", GridSearchCV(estimator=XGBClassifier(), 
                                                        param_grid=parameters,
                                                        scoring='f1', 
                                                        cv=10, 
                                                        refit=True))])
    model_pipeline.fit(X_train, y_train)

    train_pred = model_pipeline.predict(X_train)
    valid_pred = model_pipeline.predict(X_valid)

    train_acc = accuracy_score(y_true=y_train, y_pred=train_pred)
    valid_acc = accuracy_score(y_true=y_valid, y_pred=valid_pred)

    print("Train Accuracy :", train_acc)
    print("Valid Accuracy :", valid_acc)

    
    #Logging to MLflow
    try:
        
        mlflow.set_tracking_uri("http://mlflow-server:5000")
        print("Current MLflow Tracking URI:", mlflow.get_tracking_uri())
        print('MLflow tracking URI set')

        time.sleep(10)  

        model_name = "xgboost"  
        mlflow.set_experiment("billboard-hit-exp")  
        print('Experiment set with model name:', model_name)

        
        signature = mlflow.models.infer_signature(X_train, model_pipeline.predict(X_train))
        
        with mlflow.start_run():
            mlflow.log_param("model_name", model_name)
            mlflow.log_metrics({"train_acc": train_acc, "valid_acc": valid_acc})
            mlflow.sklearn.log_model(sk_model=model_pipeline, artifact_path=model_name, signature=signature, input_example=X_train.iloc[:5])

        # Close connection
        df.to_csv("/opt/airflow/data.csv", index=False)  
        conn.close() 

        return "Model and data logging complete."
    except Exception as e:
        print("Failed to log data to MLflow:", str(e))
        return "Failed to log data to MLflow"

    return


def download_model(model_name, experiment_name):
    mlflow.set_tracking_uri("http://mlflow-server:5000")
    print("Current MLflow Tracking URI:", mlflow.get_tracking_uri())

    destination_path = "/opt/airflow/dags"

    # Download model artifacts
    client = mlflow.MlflowClient()
    print('Connected!')
    # get experiment id
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment:
        experiment_id = experiment.experiment_id
    else:
        raise Exception(f"Experiment with name '{experiment_name}' not found.")

    # get the latest run by checking runs in the experiment
    runs = client.search_runs(experiment_ids=[experiment_id]) 
    if runs:
        latest_run = max(runs, key=lambda x: x.info.start_time)
    else:
        raise Exception("No runs found.")
    
    client.download_artifacts(run_id=latest_run.info.run_id, path='xgboost', dst_path='/opt/airflow/dags')

    return

load_model_dag =  DAG(
    dag_id="load-model-to-api-sever",
    schedule='0 12 * * 1',
    start_date=datetime(2023,7,20),
    catchup=False)

save_model = PythonOperator(
    task_id = 'save_model_to_registry',
    python_callable=save_model_to_registry,
    dag=load_model_dag
)

download_model = PythonOperator(
    task_id = 'download_model_from_registry',
    python_callable=download_model,
    op_kwargs={'model_name': 'xgboost', 'experiment_name': 'billboard-hit-exp'},
    dag=load_model_dag
)

copy_model = BashOperator(
    task_id='copy_model_to_server',
    bash_command='scp -o StrictHostKeyChecking=no -r -i /opt/airflow/NERDS-key.pem '
        '/opt/airflow/dags/xgboost /opt/airflow/app.py /opt/airflow/secrets.json /opt/airflow/schemas.py /opt/airflow/docker-compose.yaml /opt/airflow/Dockerfile.fastapi '
        'ubuntu@ec2-54-180-214-122.ap-northeast-2.compute.amazonaws.com:/home/ubuntu',
    dag=load_model_dag,
)


container_down = SSHOperator(
    task_id='stop_docker_container',
    ssh_conn_id='ssh_to_ec2',  
    command='docker compose down',
    dag=load_model_dag,
)   

container_up = SSHOperator(
    task_id='start_docker_container',
    ssh_conn_id='ssh_to_ec2',  
    command='nohup docker compose up --build >> /home/ubuntu/logs/$(date +%Y-%m-%d)_log.out 2>&1 &',
    dag=load_model_dag,
)   
save_model >> download_model >> copy_model >> container_down >> container_up