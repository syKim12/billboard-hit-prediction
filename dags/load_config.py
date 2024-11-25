import os
import logging

class ConfigLoader:
    def __init__(self):
        self.mongo_host_prim = os.getenv("MONGO_HOST_PRIM")
        self.mongo_host_scnd = os.getenv("MONGO_HOST_SCND")
        self.mongo_user = os.getenv("MONGO_USER")
        self.mongo_pw = os.getenv("MONGO_PW")
        self.spotify_client_id = os.getenv("SPOTIFY_CLIENT_ID")
        self.spotify_client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        self.s3_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.s3_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.tls_ca_file = os.getenv("TLS_CA_FILE")
        self.tls_cert_key_file_prim = os.getenv("TLS_CERT_KEY_FILE_PRIM")
        self.tls_cert_key_file_scnd = os.getenv("TLS_CERT_KEY_FILE_SCND")
        self.mongo_port_prim = int(os.getenv("MONGO_PORT_PRIM"))
        self.mongo_port_scnd = int(os.getenv("MONGO_PORT_SCND"))        
