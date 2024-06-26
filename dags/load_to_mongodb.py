import os, json, csv, spotipy
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

def get_update_date():
    # initial setting
    secrets = json.loads(open('/opt/airflow/dags/secrets.json').read())
    client_id = secrets["spotify_client_id"]
    client_secret= secrets["spotify_client_secret"]

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    playlist_link = "https://open.spotify.com/playlist/6UeSakyzhiEt4NB3UAd6NQ?si=511d3ed1fb874fa6"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    track_uris = [x["track"]["uri"] for x in sp.playlist_tracks(playlist_URI)["items"]]
    
    date = sp.playlist_tracks(playlist_URI)["items"][0]["added_at"].split("T")[0]

    return sp, playlist_URI, track_uris, date


def spotify_csv():
    sp, playlist_URI, track_uris, date = get_update_date()

    rank = 1
    date = sp.playlist_tracks(playlist_URI)["items"][0]["added_at"].split("T")[0]
     
    csv_filename = '/opt/airflow/csv/' + date + '_chart.csv'

    try:
        hook = MongoHook(mongo_conn_id='mongo_default')
        client = hook.get_conn()
        db = client['music']
        collection = db['chart']
        print("DB is successfully connected!")
    except:
        print("DB is not connected!")
    
    # check latest updated date
    latest_document = collection.find_one({}, sort=[('Date', -1)])
    if latest_document['Date'] == date:
        
        print('data already exists!')
        exit(0)

    with open(csv_filename, 'w', encoding='UTF-8', newline='') as csv_open:
        csv_writer = csv.writer(csv_open)
        csv_writer.writerow(('TrackID','Date','Rank','Title','Artist','Danceability','Energy','Loudness','Speechiness','Acousticness','Instrumentalness','Liveness','Valence','Tempo','Duration_ms', 'Hit', 'Followers'))

        for track in sp.playlist_tracks(playlist_URI)["items"]:
            #URI
            track_id = track["track"]["id"]
            
            #Track name
            track_name = track["track"]["name"]
            
            #Main Artist
            artist_uri = track["track"]["artists"][0]["uri"]
            artist_info = sp.artist(artist_uri)
            
            #Name, popularity, genre
            artist_name = track["track"]["artists"][0]["name"]
            artist_pop = artist_info["popularity"]
            artist_genres = artist_info["genres"]
            
            #Album
            album = track["track"]["album"]["name"]
            
            #Popularity of the track
            track_pop = track["track"]["popularity"]

            audio_features = sp.audio_features(track_id)[0]
            
            danceability = audio_features['danceability']
            energy = audio_features["energy"]
            loudness = audio_features["loudness"]
            speechiness = audio_features["speechiness"]
            acousticness = audio_features["acousticness"]
            instrumentalness = audio_features["instrumentalness"]
            liveness = audio_features["liveness"]
            valence = audio_features["valence"]
            tempo = audio_features["tempo"]
            duration_ms = audio_features["duration_ms"]

            # get popularity and number of followers of artist
            followers = artist_info["followers"]["total"]

            csv_writer.writerow([track_id, date, rank, track_name, artist_name, danceability, energy, loudness,speechiness,acousticness, instrumentalness, liveness,valence, tempo, duration_ms, 1, followers])
            rank += 1

    return csv_filename 


def load_data():
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client['music']
    collection = db['chart']
    
    val = get_update_date()
    filename = '/opt/airflow/csv/' + val[-1] + '_chart.csv'
    with open(filename, 'r') as file:
        csv_data = csv.DictReader(file)
        documents = list(csv_data)

    if documents:
        collection.insert_many(documents)

    client.close()
    return


load_csv_mongo_dag =  DAG(
    dag_id="load-csv-to-mongo",
    schedule='47 12 * * *',
    start_date=datetime(2023,7,20),
    catchup=False)

get_spotify_csv = PythonOperator(
    task_id='get-spotify-csv',
    python_callable=spotify_csv,
    dag=load_csv_mongo_dag
)

load_data = PythonOperator(
    task_id='load-csv-to-mongo',
    python_callable=load_data,
    dag=load_csv_mongo_dag
    )


    
get_spotify_csv >> load_data