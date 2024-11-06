import os

class ConfigLoader:
    def __init__(self):
        self.mongo_host = os.getenv("MONGO_HOST")
        self.mongo_user = os.getenv("MONGO_USER")
        self.mongo_pw = os.getenv("MONGO_PW")
        self.spotify_client_id = os.getenv("SPOTIFY_CLIENT_ID")
        self.spotify_client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        self.s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
        self.s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
        

