import json
import mlflow
import pandas as pd
from fastapi import FastAPI
from schemas import PredictIn, PredictOut

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

secrets = json.loads(open('/usr/app/secrets.json').read())

def get_model():
    model = mlflow.sklearn.load_model(model_uri="./sk_model")
    return model

client_id = secrets["client_id"]
client_secret= secrets["client_secret"]

client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

async def search(title, artist):
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

    track_info = await sp.search(q=artist+","+title, type="track", limit=5)
    track_id = track_info["tracks"]["items"][0]["id"]
    artist_id = track_info["tracks"]["items"][0]['artists'][0]['id']
    artist_info = sp.artist(artist_id)
    Followers.append(artist_info["followers"]["total"])
    features = sp.audio_features(track_id)[0]
    if not features == None:
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
    df = pd.DataFrame({"Followers" : Followers, "Acousticness" : Acousticness, "Danceability" : Danceability, "Duration_ms" : Duration_ms, 
                       "Energy" : Energy, "Instrumentalness" : Instrumentalness, "Liveness" : Liveness, "Loudness" : Loudness, 
                       "Speechiness" : Speechiness, "Tempo" : Tempo, "Valence" : Valence})
    return df

MODEL = get_model()

app = FastAPI()

@app.post("/predict", response_model=PredictOut)
def predict(song: PredictIn) -> PredictOut:
    df = search(song.Title, song.Artist)
    pred = MODEL.predict(df).item()
    return PredictOut(Hit=pred)