import boto3
import pickle
from os import path

BUCKET_NAME = 'data-immo'
MODEL_FILE_NAME = 'pickled_model.p'
BUCKET_NAME_TEST = 'natane-test'

def load_model():
    s3 = boto3.client('s3')
    temp_file_path = './' + MODEL_FILE_NAME
    if not path.exists(temp_file_path):
        s3.download_file(BUCKET_NAME, MODEL_FILE_NAME, temp_file_path)
    with open(temp_file_path, 'rb') as f:
        model = pickle.load(f)
    return model

def predict_model(data):
    # Load model
    model = load_model()
    # Predict class
    prediction = model.predict([
        data
    ])
    return str(round(prediction[0], 2))
