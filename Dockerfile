FROM apache/airflow:2.6.0

RUN pip3 install apache-airflow-providers-mongo 
RUN pip3 install spotipy 
RUN pip3 install apache-airflow-providers-apache-spark 
RUN pip3 install selenium
RUN pip3 install mlflow pymongo pandas scikit-learn xgboost
