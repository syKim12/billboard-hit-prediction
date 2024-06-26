FROM arm64v8/python:3.8-slim

WORKDIR /usr/app

RUN pip install -U pip &&\
    pip install mlflow==2.12.1 pandas scikit-learn==1.3.2 "fastapi[all]" spotipy==2.23.0 xgboost==2.0.3 boto3==1.26.8

COPY schemas.py schemas.py
COPY app.py app.py
COPY sk_model/ sk_model/

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--reload"]