import os
import mlflow
from argparse import ArgumentParser
import pandas as pd
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["MLFLOW_TRACKING_URI"] = "http://localhost:5001"
os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "miniostorage"

parser = ArgumentParser()
parser.add_argument("--model-name", dest="model_name", type=str, default="sk_model")
parser.add_argument("--run-id", dest="run_id", type=str)
args = parser.parse_args()

model_pipeline = mlflow.sklearn.load_model(f"runs:/{args.run_id}/{args.model_name}")

df = pd.read_csv("data.csv")

X = df.iloc[:, (df.columns != "Hit")]
object_cols = X.select_dtypes(include=['object']).columns
X.loc[:, object_cols] = X.loc[:, object_cols].astype(float)
y = df.iloc[:, -1]
y = y.astype("int")

X_train, X_valid, y_train, y_valid = train_test_split(X, y, train_size=0.7, random_state=2024)

train_pred = model_pipeline.predict(X_train)
valid_pred = model_pipeline.predict(X_valid)

train_acc = accuracy_score(y_true=y_train, y_pred=train_pred)
valid_acc = accuracy_score(y_true=y_valid, y_pred=valid_pred)

print("Train Accuracy :", train_acc)
print("Valid Accuracy :", valid_acc)