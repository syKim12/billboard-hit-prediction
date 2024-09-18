import json
import os
import mlflow
import pandas as pd
from fastapi import FastAPI
from schemas import PredictIn, PredictOut
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from concurrent.futures import ThreadPoolExecutor
import asyncio
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from boto3.session import Session
from threading import Lock

class Config:
    def __init__(self):
        self.load_secrets()

    def load_secrets(self):
        secrets_path = '/usr/app/secrets.json'
        with open(secrets_path, 'r') as file:
            secrets = json.load(file)
        self.client_id = secrets["client_id"]
        self.client_secret = secrets["client_secret"]
        self.s3_access_key_id = secrets["s3_access_key_id"]
        self.s3_secret_access_key = secrets["s3_secret_access_key"]

class SpotifyService:
    def __init__(self, config):
        self.config = config
        self.sp = self.create_spotify_client()

    def create_spotify_client(self):
        credentials_manager = SpotifyClientCredentials(
            client_id=self.config.client_id,
            client_secret=self.config.client_secret
        )
        return spotipy.Spotify(client_credentials_manager=credentials_manager)

    def search_tracks(self, title, artist):
        query = f"{artist},{title}"
        return self.sp.search(q=query, type='track', limit=5)


class ModelService:
    def __init__(self, model_path):
        self.model_path = model_path
        self.model_lock = Lock()

    def get_model(self, model_name):
        with self.model_lock:
            model_dir = os.path.join(self.model_path, model_name)
            return mlflow.sklearn.load_model(model_uri=model_dir)

async def search_spotify(title, artist):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as executor:
        followers, acousticness, danceability, duration_ms, energy, instrumentalness, liveness, loudness, speechiness, tempo, valence = [], [], [], [], [], [], [], [], [], [], []
        track_info = await loop.run_in_executor(executor, spotify_service.search_tracks, title, artist)
        track_id = track_info["tracks"]["items"][0]["id"]
        artist_id = track_info["tracks"]["items"][0]['artists'][0]['id']
        artist_info = spotify_service.sp.artist(artist_id)
        followers.append(artist_info["followers"]["total"])
        features = spotify_service.sp.audio_features(track_id)[0]
        if features is not None:
            danceability.append(features["danceability"])
            energy.append(features["energy"])
            loudness.append(features["loudness"])
            speechiness.append(features["speechiness"])
            acousticness.append(features["acousticness"])
            instrumentalness.append(features["instrumentalness"])
            liveness.append(features["liveness"])
            valence.append(features["valence"])
            tempo.append(features["tempo"])
            duration_ms.append(features["duration_ms"])
        df = pd.DataFrame({
            "Followers": followers, 
            "Acousticness": acousticness, 
            "Danceability": danceability, 
            "Duration_ms": duration_ms,
            "Energy": energy, 
            "Instrumentalness": instrumentalness, 
            "Liveness": liveness, 
            "Loudness": loudness,
            "Speechiness": speechiness, 
            "Tempo": tempo, 
            "Valence": valence
        })
        return df

app = FastAPI()
config = Config()
spotify_service = SpotifyService(config)
model_service = ModelService('/usr/models')

@app.post("/predict", response_model=PredictOut)
async def predict(song: PredictIn) -> PredictOut:
    df = await search_spotify(song.Title, song.Artist)
    MODEL = model_service.get_model('xgboost')
    pred = MODEL.predict(df).item()
    return PredictOut(Hit=pred)

@app.get("/download-latest-model/")
async def download_latest_model():
    session = Session(
        aws_access_key_id=config.s3_access_key_id,
        aws_secret_access_key=config.s3_secret_access_key,
        region_name='ap-northeast-2'
    )
    s3 = session.client('s3')

    bucket = 'nerds-model'
    prefix = '5/'  

    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = response.get('Contents', [])

        if not files:
            return {"status": "error", "message": "No files found"}

        # get the latest file
        latest_file = max(files, key=lambda x: x['LastModified'])
        runid_prefix = '/'.join(latest_file['Key'].split('/')[:-2]) 

        # get objects under the latest directory
        response = s3.list_objects_v2(Bucket=bucket, Prefix=runid_prefix)
        artifacts = response.get('Contents', [])

        if not artifacts:
            return {"status": "error", "message": "No artifacts found"}

        # Download each artifact file
        with model_service.model_lock:
            for artifact in artifacts:
                file_key = artifact['Key']
                file_path = os.path.join(model_service.model_path, *file_key.split('/')[-2:]) 
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                s3.download_file(Bucket=bucket, Key=file_key, Filename=file_path)

        return {"status": "success", "message": f"All files from {runid_prefix} downloaded."}

    except NoCredentialsError:
        return {"status": "error", "message": "AWS credentials are not valid."}
    except ClientError as e:
        return {"status": "error", "message": str(e)}
    except Exception as e:
        return {"status": "error", "message": str(e)}