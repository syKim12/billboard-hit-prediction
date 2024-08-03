## Overview
This project utilizes Spotify's API to collect various features of songs to train a machine learning model that predicts whether a song will make it into the Billboard Hot 100 chart. The project automates the process of data collection, model training, and provides an API endpoint for making predictions.



## Make predictions
### Request
To make a prediction, send a POST request to the '/predict' endpoint with the song title and artist name. 

Below is an example using the song "Good Day" by IU.


'''bash
curl -X POST http://ec2-54-180-214-122.ap-northeast-2.compute.amazonaws.com:8000/predict -H  "Content-Type: application/json" -d '{"Title": "Good day", "Artist": "IU"}'
'''
### Response
The API will return a prediction result, which shows whether the song is likely to make it into the Billboard Hot 100 chart. 

For example, 'Hit: 1' indicates that the song has a higher probability of charting on the Billboard Hot 100.

## How it works
1. Data Collection: The system pulls song features from Spotify based on input parameters (song title and artist). 

    This data collection process is automated using Apache Airflow
2. Model Training: The model is periodically retrained on new data to keep predictions relevant and accurate. 

    MLflow is utilized to track experiments, manage and store models, and log their parameters and metrics. 
3. Prediction: The trained model predicts the probability of a song making it to the Billboard Hot 100.

    The model is served through a FastAPI endpoint, which is implemented asynchronously.
