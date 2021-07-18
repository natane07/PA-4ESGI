from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import numpy as np
import pandas as pd
import pickle
import boto3
from os import path
import config

def train_model(df_ml):
    """
    Entrainement du modèle RandomForeste avec les données immobiliere
    :param df_ml: Les données immobilieres
    :return: None
    """
    # récupération des variables pour le modèle
    df_ml = df_ml[['latitude', 'longitude', 'surface_reelle_bati', 'prix_metre_carre', 'code_region', 'prix_moyen_cartier']]

    # récupération des input et output
    y = df_ml['prix_metre_carre'].values
    X = df_ml.drop(columns=['prix_metre_carre']).values

    # Split des données
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,random_state=12)
    # Entrainement du modèle
    forest = RandomForestRegressor(n_estimators=40,min_samples_split=5)
    forest.fit(X_train,y_train)
    # Graphqiue de la répartition des erreur du modèle
    shema = pd.DataFrame((np.abs(y_test - forest.predict(X_test)) / y_test)) * 100
    shema[shema < 200].hist(bins=1000)
    err_model = shema.iloc[0].astype(float).median()
    print("% d'erreur modele: ", err_model)

    # Sauvegarde du modèle
    name_file_model = "pickled_model_" + str(round(err_model, 2)) + ".p"
    pickle.dump(forest, open("./" + name_file_model, "wb"))
    # Upload du modèle dans S3
    load_file_in_s3(name_file_model, name_file_model)


def load_file_in_s3(name_file, path_aws):
    """
    Upload d'un fichier dans S3
    :param name_file: Nom du fichier
    :param path_aws: Path du fichier dans AWS
    :return: None
    """
    # ouverture d'une sessions S3
    conn = boto3.session.Session(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    s3 = conn.client('s3')
    temp_file_path = './' + name_file
    if path.exists(temp_file_path):
        # lecture du fichier et Uplaod dans le bucket S3
        with open(temp_file_path, "rb") as f:
            s3.upload_fileobj(f, config.BUCKET_NAME, path_aws)