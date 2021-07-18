import urllib.request
import pandas as pd
import os
import boto3
from os import path
from sklearn.neighbors import BallTree
import numpy as np
import time
import pickle
import config
import datetime

def download_files_etalab():
    """
    Téléchargement des données immobilieres des 5 dernieres années depuis data-gouv
    :return: None
    """
    date = datetime.datetime.now()
    yearNow = date.year
    for i in range(1, 6):
        year = str(yearNow - i)
        print(year)
        url_files = "https://files.data.gouv.fr/geo-dvf/latest/csv/" + year + "/full.csv.gz"
        name_file = "full-" + year + ".csv.gz"
        path_file = './data/' + name_file
        urllib.request.urlretrieve(url_files, path_file)

def download_file_region():
    """
    Téléchargement du dataset des regions et département depuis S3
    :return: None
    """
    conn = boto3.session.Session(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    s3 = conn.client('s3')
    temp_file_path = './data/' + config.REGION_FILE_NAME
    if not path.exists(temp_file_path):
        s3.download_file(config.BUCKET_NAME, config.REGION_FILE_NAME, temp_file_path)

def read_file_region():
    """
    Lecture du fichier des regions
    :return: le dataset des regions
    """
    file_regions = "./data/departments.csv"
    return pd.read_csv(file_regions, low_memory = True)

def import_files_in_dict(year):
    """
    Import du dataset des données immobilieres de l'année donnée
    :param year: L'année
    :return: Le dataframe des données
    """
    path_file = './data/full-' + year + '.csv.gz'
    df = pd.read_csv(path_file, compression='gzip', low_memory = True)
    return df

def filter_data(df):
    """
    Filtrage des données immobilieres
    :param df: le dataset des données filtrer
    :return: Le dataframe des données
    """
    df = df[ (df['nature_mutation'] == 'Vente') | (df['nature_mutation'] == "Vente en l'état futur d'achèvement")]
    df = df[ (df['valeur_fonciere'].notna()) & (df['valeur_fonciere'] != 0)]
    df = df[ (df['surface_reelle_bati'].notna()) & (df['surface_reelle_bati'] != 0)]
    df = df[ (df['longitude'].notna())]
    df = df[ (df['latitude'].notna()) ]
    df = df[ (df['nombre_lots'] == 0) ]
    df = df[ (df['code_departement'].notna()) ]
    return df

def group_by_data(df):
    """
    Regroupement des données qui sont divisé
    :param df: le dataframe des données
    :return: le dataframe des données regroupé
    """
    col_gp = ['id_mutation', 'date_mutation', 'nature_mutation', 'code_postal', 'code_commune', 'code_region', 'code_departement',
            'type_local', 'latitude', 'longitude']
    return df.groupby(col_gp, as_index=False).agg( { 'valeur_fonciere': 'max', 'surface_reelle_bati': 'max', 'nombre_pieces_principales': 'max'})

def data_clean():
    """
    Nétoyage et traitement des données
    :return: les données traité et clean
    """
    output_data = ['id_mutation', 'date_mutation', 'nature_mutation',
                   'valeur_fonciere', 'code_postal', 'code_commune', 'code_region',
                   'code_departement', 'code_type_local', 'type_local',
                   'surface_reelle_bati', 'nombre_pieces_principales', 'latitude', 'longitude']
    # Création du tableau des 5 dernieres années courantes
    date = datetime.datetime.now()
    yearNow = date.year
    years_files = []
    for i in range(1, 6):
        year = str(yearNow - i)
        years_files.append(year)

    df_immo = []
    # Teléchargement du dataset des départements et regions
    regions = read_file_region()
    # Traitement des données immobilieres pour les 5 dernieres années
    for year in years_files:
        # récupération des données immobilieres de l'année donnée
        df = import_files_in_dict(year)
        # transformation du code departement pour le merge avec l'autre dataset
        df["code_departement"] = df["code_departement"].astype(str)
        df["code_departement"] = df["code_departement"].apply(lambda x: '0' + x if len(x) == 1 else x)
        df = pd.merge(df, regions, how='left', left_on='code_departement', right_on='code')
        df["code_region"] = df["region_code"]
        # filtrage des données
        df_clean = filter_data(df)
        # recupération des variables intéressantes
        df_clean = df_clean[output_data]
        # regroupement des données
        df_clean = group_by_data(df_clean)
        # Création de la variable prix_metre_carré par bien
        df_clean["prix_metre_carre"] = (df_clean['valeur_fonciere'] / df_clean['surface_reelle_bati']).apply(
            lambda x: round(x))
        df_clean = df_clean.sort_values(['code_region', 'latitude', 'longitude'])
        # Concaténation des données de chaque année
        if len(df_immo) == 0:
            df_immo = df_clean
        else:
            df_immo = pd.concat([df_immo, df_clean])
    return df_immo

def data_prep(df_immo):
    """
    Filtrage des données et création des modèles de voisinage
    :param df_immo: Le dataframe des données
    :return: Le dataframe des données filtrer
    """
    # Appartement
    # df_immo_appart = df_immo[(df_immo["nature_mutation"] == "Vente") & (df_immo["type_local"] == "Appartement")]
    # Maison
    # df_immo_maison = df_immo[(df_immo["nature_mutation"] == "Vente") & (df_immo["type_local"] == "Maison")]

    # Récupération des données de ventes immobilieres et de type maison ou appartement
    df_immo_maison = df_immo[(df_immo["nature_mutation"] == "Vente") & ((df_immo["type_local"] == "Appartement") | (df_immo["type_local"] == "Maison"))]
    # Génération des modèles Balltree de chaque regions
    return create_all_model_ball_tree_region(df_immo_maison)

def load_file_in_s3(name_file, path_aws):
    """
    Upload d'un fichier sur S3
    :param name_file: Nom du fichier
    :param path_aws: Chemin du fichier
    :return: None
    """
    conn = boto3.session.Session(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    s3 = conn.client('s3')
    temp_file_path = './' + name_file
    if path.exists(temp_file_path):
        with open(temp_file_path, "rb") as f:
            s3.upload_fileobj(f, config.BUCKET_NAME, path_aws)

def download_file_s3(path_file_aws, name_file):
    """
    Téléchargement d'un fichier sur S3
    :param path_file_aws: Chemin du fichier sur S3
    :param name_file: Nom du fichier
    :return:
    """
    conn = boto3.session.Session(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    s3 = conn.client('s3')
    temp_file_path = './' + name_file
    s3.download_file(config.BUCKET_NAME, path_file_aws, temp_file_path)


def create_all_model_ball_tree_region(df_immo_maison):
    """
    Création du modèle BallTree pour chaque région de france
    :param df_immo_maison: La dataframe des données
    :return: Les données préparer pour le modèle ML
    """
    # Création de la variable distance moyenne d'un bien avec un tableau de la taille du dataset
    df_immo_maison['distance_moyenne'] = np.zeros(len(df_immo_maison))
    models = {}
    # récupération des regions de france
    regions = df_immo_maison.code_region.unique()
    df_prepare_ml = []
    # Création du modèle pour chaque région de france
    for k in range(len(regions)):
        start = time.time()
        # filtrage des données pour la region concerné
        data_region = df_immo_maison[df_immo_maison.code_region == regions[k]]
        data_region = data_region.reset_index(drop=True)
        # Création du modèle à partir des données latitude et longitude de chaque bien
        models[k] = BallTree(data_region[['latitude', 'longitude']].values, leaf_size=2, metric='haversine')

        # Sauvegarde du modèle et upload sur S3
        name_file_model = "model_balltree_region_" + str(regions[k]) + ".p"
        pickle.dump(models[k], open("./model_balltree/" + name_file_model, "wb"))
        load_file_in_s3("model_balltree/" + name_file_model, "model_balltree/" + name_file_model)

        # Récupération des données pour la région concerné
        data_region = df_immo_maison[df_immo_maison.code_region == regions[k]]
        data_region = data_region.reset_index(drop=True)
        # récupération de la distance moyenne des 10 biens les plus proche pour chaque région ainsi que l'indice des 10 bien les plus proches
        dist, indices = models[k].query(data_region[['latitude', 'longitude']].values, k=10)
        # Conversion de la distance de Haversine en Kilometre
        data_region['distance_moyenne'] = np.mean(dist[:, 1:] * 6371, 1)
        df_prix_metre_carre = pd.DataFrame()
        df_prix_metre_carre['prix_metre_carre'] = np.zeros(len(data_region))
        # Calcule de la valeur du prix moyen du metre carré des 10 biens les plus proches (du cartier)
        for i in range(1, 10):
            df_prix_metre_carre += pd.DataFrame(data_region.iloc[indices[:, i], :]['prix_metre_carre']).reset_index(drop=True)
        df_prix_metre_carre = df_prix_metre_carre / 10
        data_region['prix_moyen_cartier'] = df_prix_metre_carre.values
        data_region.sort_values(['id_mutation', 'latitude', 'longitude'])

        # Sauvegarde du fichier des données de la regions avec le prix moyen du cartier
        data_region.to_csv("./data-region/data_region_" + regions[k] + ".csv", index=False, header=True)
        # Upload du fichier sur S3
        load_file_in_s3("data-region/data_region_" + regions[k] + ".csv", "data-region/data_region_" + regions[k] + ".csv")

        # Concaténation des fichier de chaque régions
        if len(df_prepare_ml) == 0:
            df_prepare_ml = data_region
        else:
            df_prepare_ml = pd.concat([df_prepare_ml, data_region])
        stop = time.time()
        print(stop - start)

    return df_prepare_ml[['latitude', 'longitude', 'surface_reelle_bati', 'prix_metre_carre', 'code_region','prix_moyen_cartier']]

def execute_script_to_prepare_data():
    """
    Execution de l'ensemble du script du traitement des données et génaration des modèle de voisinage
    :return: Les données préparé pour l'entrainement du modèle
    """
    # Teléchargement des données
    download_files_etalab()
    # Telechargement du dataset des regions
    download_file_region()
    # Netoyyage des données
    df_immo = data_clean()
    # Preparation des données et creation des modeles balltree
    return data_prep(df_immo)
