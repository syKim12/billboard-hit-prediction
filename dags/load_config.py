import os
import logging

class ConfigLoader:
    def __init__(self):
        self.mongo_host = self.get_variable("MONGO_HOST")
        self.mongo_user = self.get_variable("MONGO_USER")
        self.mongo_pw = self.get_variable("MONGO_PW")
        self.spotify_client_id = self.get_variable("SPOTIFY_CLIENT_ID")
        self.spotify_client_secret = self.get_variable("SPOTIFY_CLIENT_SECRET")
        self.s3_access_key_id = self.get_variable("S3_ACCESS_KEY_ID")
        self.s3_secret_access_key = self.get_variable("S3_SECRET_ACCESS_KEY")

    def get_variable(self, name):
        value = os.getenv(name)
        if value is None:
            logging.error(f"Environment variable {name} is not set.")
            raise ValueError(f"Environment variable {name} is required but not set.")
        return value
