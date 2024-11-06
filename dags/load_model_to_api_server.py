import os, json, csv, spotipy, time, torch
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
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
from load_env import EnvLoader

env = EnvLoader()
env.load_env()

def save_model_to_registry():
    # 0. set mlflow environments
    
    mongo_host = env.get_env_variable("MONGO_HOST")
    mongo_user = env.get_env_variable("MONGO_USER")
    mongo_pw = env.get_env_variable("MONGO_PW")

    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://s3.ap-northeast-2.amazonaws.com"
    os.environ["MLFLOW_TRACKING_URI"] = "http://mlflow-server:5000"
    os.environ["AWS_ACCESS_KEY_ID"] = env.get_env_variable("S3_ACCESS_KEY_ID")
    os.environ["AWS_SECRET_ACCESS_KEY"] = env.get_env_variable("S3_SECRET_ACCESS_KEY")

    
    mlflow.set_tracking_uri(env.get_env_variable("MLFLOW_TRACKING_URI"))
    print("MLFLOW_S3_ENDPOINT_URL:", env.get_env_variable("MLFLOW_S3_ENDPOINT_URL"))
    print("MLFLOW_TRACKING_URI:", env.get_env_variable("MLFLOW_TRACKING_URI"))


    # 1. get data
    conn = pymongo.MongoClient(host=mongo_host, 
                            port=27017, 
                            username=mongo_user, 
                            password=mongo_pw)
    db_name = "music"
    db = conn.get_database(db_name)
    chart = db.get_collection("chart")
    non_chart = db.get_collection("non_chart")
    chart_df = pd.DataFrame(list(chart.find()))
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
        model_name = "xgboost"  
        mlflow.set_experiment("billboard-hit-xgb-exp")  
        print('Experiment set with model name:', model_name)

        
        signature = mlflow.models.infer_signature(X_train, model_pipeline.predict(X_train))
        
        with mlflow.start_run():
            mlflow.log_param("model_name", model_name)
            mlflow.log_metrics({"train_acc": train_acc, "valid_acc": valid_acc})
            mlflow.sklearn.log_model(sk_model=model_pipeline, artifact_path=model_name, signature=signature, input_example=X_train.iloc[:5])

        DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")
        date_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        file_path = os.path.join(DATA_DIR, f"data_{date_str}.csv")

        # Close connection
        df.to_csv(file_path, index=False)  
        conn.close() 

        return "Model and data logging complete."
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

    return



load_model_dag =  DAG(
    dag_id="load-model-to-api-sever",
    schedule=None,
    start_date=datetime(2023,7,20),
    catchup=False)

save_model = PythonOperator(
    task_id = 'save_model_to_registry',
    python_callable=save_model_to_registry,
    dag=load_model_dag
)


trigger_latest_model_download = SimpleHttpOperator(
    task_id='trigger_latest_model_download',
    http_conn_id='http_fastapi',
    endpoint='download-latest-model/',
    method='GET',
    response_check=lambda response: "success" in response.text,
    dag=load_model_dag
)

save_model >> trigger_latest_model_download 