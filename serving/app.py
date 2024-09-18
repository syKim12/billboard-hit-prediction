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

model_lock = Lock()
secrets = json.loads(open('/usr/app/secrets.json').read())
model_path = '/usr/models'

def get_model(model_name):
    with model_lock:
        model_dir = os.path.join(model_path, model_name)
        model = mlflow.sklearn.load_model(model_uri=model_dir)
        return model

client_id = secrets["client_id"]
client_secret = secrets["client_secret"]

client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def search_spotify(title, artist): # Sync, since spotipy doens't support async
    Followers = []
    Acousticness = []
    Danceability = []
    Duration_ms = []
    Energy = []
    Instrumentalness = []
    Liveness = []
    Loudness = []
    Speechiness = []
    Tempo = []
    Valence = []

    track_info = sp.search(q=artist + "," + title, type="track", limit=5)
    track_id = track_info["tracks"]["items"][0]["id"]
    artist_id = track_info["tracks"]["items"][0]['artists'][0]['id']
    artist_info = sp.artist(artist_id)
    Followers.append(artist_info["followers"]["total"])
    features = sp.audio_features(track_id)[0]
    if features is not None:
        Danceability.append(features["danceability"])
        Energy.append(features["energy"])
        Loudness.append(features["loudness"])
        Speechiness.append(features["speechiness"])
        Acousticness.append(features["acousticness"])
        Instrumentalness.append(features["instrumentalness"])
        Liveness.append(features["liveness"])
        Valence.append(features["valence"])
        Tempo.append(features["tempo"])
        Duration_ms.append(features["duration_ms"])
    df = pd.DataFrame({
        "Followers": Followers, 
        "Acousticness": Acousticness, 
        "Danceability": Danceability, 
        "Duration_ms": Duration_ms,
        "Energy": Energy, 
        "Instrumentalness": Instrumentalness, 
        "Liveness": Liveness, 
        "Loudness": Loudness,
        "Speechiness": Speechiness, 
        "Tempo": Tempo, 
        "Valence": Valence
    })
    return df

async def search(title, artist):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as executor:
        result = await loop.run_in_executor(executor, search_spotify, title, artist)
        return result


app = FastAPI()

@app.post("/predict", response_model=PredictOut)
async def predict(song: PredictIn) -> PredictOut:
    MODEL = get_model('xgboost')
    df = await search(song.Title, song.Artist)
    pred = MODEL.predict(df).item()
    return PredictOut(Hit=pred)

@app.get("/download-latest-model/")
async def download_latest_model():
    session = Session(
        aws_access_key_id=secrets["s3_access_key_id"],
        aws_secret_access_key=secrets["s3_secret_access_key"],
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
        with model_lock:
            for artifact in artifacts:
                file_key = artifact['Key']
                file_path = os.path.join(model_path, *file_key.split('/')[-2:])  # Adjust path as needed
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                s3.download_file(Bucket=bucket, Key=file_key, Filename=file_path)

        return {"status": "success", "message": f"All files from {runid_prefix} downloaded."}

    except NoCredentialsError:
        return {"status": "error", "message": "AWS credentials are not valid."}
    except ClientError as e:
        return {"status": "error", "message": str(e)}
    except Exception as e:
        return {"status": "error", "message": str(e)}