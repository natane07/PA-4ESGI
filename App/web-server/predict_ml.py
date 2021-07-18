import boto3
import pickle
from os import path
import data_prep
import pandas as pd
import numpy as np
import config

def load_data_region():
    """
    Création du dataframe avec toutes les données des regions
    :return: le dataframe avec les données des regions
    """
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
    """
    Préparation des données pour le modèle ML de prediction
    :param body_dict: le dictionaire avec les données du body JSON
    :return: un dataframe pandas avec les données néccesaire pour le modèle
    """
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

    # Télechargement et lecture du datarame des regions et départements
    data_prep.download_file_region()
    regions = data_prep.read_file_region()

    # Recupere le code region par rapport au code departement
    data_predict["code_departement"] = data_predict["code_departement"].astype(str)
    data_predict["code_departement"] = data_predict["code_departement"].apply(lambda x: '0' + x if len(x) == 1 else x)
    data_predict = pd.merge(data_predict, regions, how='left', left_on='code_departement', right_on='code')
    data_predict["code_region"] = data_predict["region_code"]

    # Récupération du modèle Balltree sur S3 et chargement du modele Balltree de la region concerner
    code_region = data_predict["code_region"].values[0]
    name_file_model = "model_balltree_region_" + str(code_region) + ".p"
    data_prep.download_file_s3("model_balltree/" + name_file_model, "model_balltree/" + name_file_model)
    model_balltree = load_model_balltree("./model_balltree/" + name_file_model)

    # Chargement des données de la région concerné
    data_prep.download_file_s3("data-region/data_region_" + code_region + ".csv", "data-region/data_region_" + code_region + ".csv")
    file_regions = "./data-region/data_region_" + code_region + ".csv"
    data_region = pd.read_csv(file_regions, low_memory = True)

    # Utilisation du modéle balltree
    dist, indices = model_balltree.query(data_predict[['latitude', 'longitude']].values, k=10)
    # Calcul de la distance moyenne (multiplier par la circonference de la terre pour transformer en Km)
    data_predict['distance_moyenne'] = np.mean(dist[:, 1:] * 6371, 1)
    a = pd.DataFrame()
    a['prix_metre_carre'] = np.zeros(len(data_predict))
    # Moyenne des prix au mettre carré des 10 bien les plus proches
    for i in range(1, 10):
        a += pd.DataFrame(data_region.iloc[indices[:, i], :]['prix_metre_carre']).reset_index(drop=True)
    a = a / 10
    data_predict['prix_moyen_cartier'] = a.values

    return data_predict

def load_model_balltree(temp_file_path):
    """
    Chargement du modèle en mémoire
    :param temp_file_path: Path du modèle
    :return: le modèle chargé
    """
    with open(temp_file_path, 'rb') as f:
        model = pickle.load(f)
    return model


def load_model():
    """
    Téléchargement du modèle depuis S3 et chargement en mémoire du modèle
    :return: Le modèle ML chargé en mémoire
    """
    # Création d'une session S3
    conn = boto3.session.Session(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    s3 = conn.client('s3')
    temp_file_path = './' + config.MODEL_ML_FILE_NAME
    # Télécgargement des du modèle
    if not path.exists(temp_file_path):
        s3.download_file(config.BUCKET_NAME, config.MODEL_ML_FILE_NAME, temp_file_path)
    # Chargement du modèle
    with open(temp_file_path, 'rb') as f:
        model = pickle.load(f)
    return model

def predict_model(data):
    """
    Utiliation du modèle pour la prédiction
    :param data: les données pour la prediction
    :return: Le prix du bien au metre carré
    """
    # Load model
    model = load_model()
    # Predict model
    prediction = model.predict([
        data
    ])
    return str(round(prediction[0], 2))
