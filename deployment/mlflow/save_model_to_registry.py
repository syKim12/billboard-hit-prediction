import os
import json
import mlflow
from argparse import ArgumentParser
import pandas as pd
import pymongo
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier

# 0. set mlflow environments
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["MLFLOW_TRACKING_URI"] = "http://localhost:5001"
os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "miniostorage"

secrets = json.loads(open('/Users/jb/Documents/billboard-hit-prediction/deployment/secrets.json').read())

# 1. get data
conn = pymongo.MongoClient(host=secrets["mongo_host"], 
                           port=27017, 
                           username=secrets["mongo_user"], 
                           password=secrets["mongo_pw"])
db_name = "music"
db = conn.get_database(db_name)
chart = db.get_collection("chart")
non_chart = db.get_collection("non_chart")
chart_df = pd.DataFrame(list(chart.find()))
chart_df = chart_df[chart_df["Date"].isna() == False]
non_chart_df = pd.DataFrame(list(non_chart.find()))
columns = ["Followers", "Acousticness", "Danceability", "Duration_ms", "Energy", 
           "Instrumentalness", "Liveness", "Loudness", "Speechiness", "Tempo", "Valence", "Hit"]
chart_df = chart_df[columns].dropna()
non_chart_df = non_chart_df[columns]
df = pd.concat([chart_df, non_chart_df], axis=0)

X = df.iloc[:, (df.columns != "Hit")]
object_cols = X.select_dtypes(include=['object']).columns
X.loc[:, object_cols] = X.loc[:, object_cols].astype(float)
y = df.iloc[:, -1]
y = y.astype("int")

X_train, X_valid, y_train, y_valid = train_test_split(X, y, train_size=0.7, random_state=2024)



# 2. model development and train
parameters={'max_depth':[1,3,5,10],
            'n_estimators':[100,200,300],
            'learning_rate':[1e-3,0.01,0.1,1],
            'random_state':[1]}

model_pipeline = Pipeline([("scaler", StandardScaler()), 
                           ("classifier", GridSearchCV(estimator=XGBClassifier(), 
                                                       param_grid=parameters,
                                                       scoring='f1', 
                                                       cv=10, 
                                                       refit=True))])
model_pipeline.fit(X_train, y_train)

train_pred = model_pipeline.predict(X_train)
valid_pred = model_pipeline.predict(X_valid)

train_acc = accuracy_score(y_true=y_train, y_pred=train_pred)
valid_acc = accuracy_score(y_true=y_valid, y_pred=valid_pred)

print("Train Accuracy :", train_acc)
print("Valid Accuracy :", valid_acc)

# 3. save model
parser = ArgumentParser()
parser.add_argument("--model-name", dest="model_name", type=str, default="sk_model")
args = parser.parse_args()

mlflow.set_experiment("new-exp")

signature = mlflow.models.signature.infer_signature(model_input=X_train, model_output=train_pred)
input_sample = X_train.iloc[:10]

with mlflow.start_run():
    mlflow.log_metrics({"train_acc": train_acc, "valid_acc": valid_acc})
    mlflow.sklearn.log_model(
        sk_model=model_pipeline,
        artifact_path=args.model_name,
        signature=signature,
        input_example=input_sample,
    )

# 4. save data
df.to_csv("data.csv", index=False)