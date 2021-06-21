import boto3
import pickle
from os import path
import data_prep
import pandas as pd
import numpy as np
import config
import joblib

def load_data_region():
    liste_code_regions = ['01', '02', '03', '04', '11', '24', '27', '28', '32', '44', '52', '53', '75', '76', '84', '93', '94']

    df_immo_region = []
    for code_region in liste_code_regions:
        file_regions = "./data-region/data_region_" + code_region + ".csv"
        df_regions = pd.read_csv(file_regions, low_memory = True)

        if len(df_immo_region) == 0:
            df_immo_region = df_regions
        else:
            df_immo_region = pd.concat([df_immo_region, df_regions])
    return df_immo_region

def prepare_data(body_dict):
    # Extraction des données depuis le Json
    latitude = str(body_dict['latitude'])
    longitude = str(body_dict['longitude'])
    surface_reelle_bati = str(body_dict['surface_reelle_bati'])
    code_departement = str(body_dict['code_departement'])

    # Création du Dataframe
    data_predict = pd.DataFrame()
    data_predict['latitude'] = [latitude]
    data_predict['longitude'] = [longitude]
    data_predict['surface_reelle_bati'] = [surface_reelle_bati]
    data_predict['code_departement'] = [code_departement]

    # Télechargement et lecture du datarame des regions
    data_prep.download_file_region()
    regions = data_prep.read_file_region()

    # Recupere le code region par rapport au code departement
    data_predict["code_departement"] = data_predict["code_departement"].astype(str)
    data_predict["code_departement"] = data_predict["code_departement"].apply(lambda x: '0' + x if len(x) == 1 else x)
    data_predict = pd.merge(data_predict, regions, how='left', left_on='code_departement', right_on='code')
    data_predict["code_region"] = data_predict["region_code"]

    # Chargement du modele Balltree de la region concerner
    code_region = data_predict["code_region"].values[0]
    name_file_model = "model_balltree_region_" + str(code_region) + ".p"
    data_prep.download_file_s3("model_balltree/" + name_file_model, "model_balltree/" + name_file_model)
    model_balltree = load_model_balltree("./model_balltree/" + name_file_model)

    # Chargement des données de la région
    data_prep.download_file_s3("data-region/data_region_" + code_region + ".csv", "data-region/data_region_" + code_region + ".csv")
    file_regions = "./data-region/data_region_" + code_region + ".csv"
    data_region = pd.read_csv(file_regions, low_memory = True)
    # data_region = load_data_region()

    # Utilisation du modéle
    dist, indices = model_balltree.query(data_predict[['latitude', 'longitude']].values, k=10)
    data_predict['distance_moyenne'] = np.mean(dist[:, 1:] * 6341, 1)
    a = pd.DataFrame()
    a['prix_metre_carre'] = np.zeros(len(data_predict))
    for i in range(1, 10):
        a += pd.DataFrame(data_region.iloc[indices[:, i], :]['prix_metre_carre']).reset_index(drop=True)
    a = a / 10
    data_predict['prix_moyen_cartier'] = a.values

    return data_predict

def load_model_balltree(temp_file_path):
    with open(temp_file_path, 'rb') as f:
        model = pickle.load(f)
    return model


def load_model():
    conn = boto3.session.Session(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    s3 = conn.client('s3')
    temp_file_path = './' + config.MODEL_ML_FILE_NAME
    if not path.exists(temp_file_path):
        s3.download_file(config.BUCKET_NAME, config.MODEL_ML_FILE_NAME, temp_file_path)
        model = joblib.load(temp_file_path)
    # with open(temp_file_path, 'rb') as f:
    #     model = pickle.load(f)
    return model

def predict_model(data):
    # Load model
    model = load_model()
    # Predict class
    prediction = model.predict([
        data
    ])
    return str(round(prediction[0], 2))
