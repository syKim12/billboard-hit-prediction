import unittest

import os, json, csv, spotipy
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
#from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime,timedelta
from spotipy.oauth2 import SpotifyClientCredentials
from airflow.operators.dagrun_operator import TriggerDagRunOperator
#import mlflow
#from argparse import ArgumentParser
import pandas as pd
import pymongo
#from sklearn.metrics import accuracy_score
#from sklearn.model_selection import train_test_split
#from sklearn.preprocessing import StandardScaler
#from sklearn.pipeline import Pipeline
#from sklearn.model_selection import GridSearchCV
#from xgboost import XGBClassifier

def get_update_date():
    # initial setting
    secrets = json.loads(open('/Users/sooyeon/airflow/secrets.json').read())
    client_id = secrets["spotify_client_id"]
    client_secret= secrets["spotify_client_secret"]

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    playlist_link = "https://open.spotify.com/playlist/6UeSakyzhiEt4NB3UAd6NQ?si=511d3ed1fb874fa6"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    track_uris = [x["track"]["uri"] for x in sp.playlist_tracks(playlist_URI)["items"]]
    
    date = sp.playlist_tracks(playlist_URI)["items"][0]["added_at"].split("T")[0]

    return sp, playlist_URI, track_uris, date

def getAudioFeatures(sp, track_ids):
    features = []
    for i in range(0, len(track_ids), 100):
        batch = track_ids[i:i+100]
        features.extend(sp.audio_features(batch))
    return features


def writeCsv():
    sp, playlist_URI, track_uris, date = get_update_date()

    playlist_tracks = sp.playlist_tracks(playlist_URI)["items"]
    track_ids = [track["track"]["id"] for track in playlist_tracks]
    
    audio_features = getAudioFeatures(sp, track_ids)

    #with open(csv_filename, 'w', encoding='UTF-8', newline='') as csv_open:
    #csv_writer = csv.writer(csv_open)
    #csv_writer.writerow(('TrackID','Date','Rank','Title','Artist','Danceability','Energy','Loudness','Speechiness','Acousticness','Instrumentalness','Liveness','Valence','Tempo','Duration_ms', 'Hit', 'Followers'))

    for i, track in enumerate(playlist_tracks):
        track_id = track["track"]["id"]
        track_name = track["track"]["name"]
        artist_name = track["track"]["artists"][0]["name"]
        
        features = audio_features[i]
        print(track_name, artist_name)
    return

class Test(unittest.TestCase):
    def testCsv(self):
        writeCsv()


if __name__ == '__main__':
    unittest.main()