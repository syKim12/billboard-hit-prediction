import json
import mlflow
import pandas as pd
from fastapi import FastAPI
from schemas import PredictIn, PredictOut
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from concurrent.futures import ThreadPoolExecutor
import asyncio

secrets = json.loads(open('/usr/app/secrets.json').read())

def get_model():
    model = mlflow.sklearn.load_model(model_uri="./sk_model")
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

MODEL = get_model()

app = FastAPI()

@app.post("/predict", response_model=PredictOut)
async def predict(song: PredictIn) -> PredictOut:
    df = await search(song.Title, song.Artist)
    pred = MODEL.predict(df).item()
    return PredictOut(Hit=pred)
