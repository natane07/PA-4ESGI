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

def download_files_etalab():
  years_files = ['2016', '2017', '2018', '2019', '2020']
  for year in years_files:
    url_files = "https://files.data.gouv.fr/geo-dvf/latest/csv/" + year + "/full.csv.gz"
    name_file = "full-" + year +".csv.gz"
    path_file = './data/' + name_file
    urllib.request.urlretrieve(url_files, path_file)

def download_file_region():
    conn = boto3.session.Session(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    s3 = conn.client('s3')
    temp_file_path = './data/' + config.REGION_FILE_NAME
    if not path.exists(temp_file_path):
        s3.download_file(config.BUCKET_NAME, config.REGION_FILE_NAME, temp_file_path)

def read_file_region():
    file_regions = "./data/departments.csv"
    return pd.read_csv(file_regions)

def import_files_in_dict(year):
  path_file = './data/full-' + year + '.csv.gz'
  df = pd.read_csv(path_file, compression='gzip')
  return df

def filter_data(df):
  df = df[ (df['nature_mutation'] == 'Vente') | (df['nature_mutation'] == "Vente en l'état futur d'achèvement")]
  df = df[ (df['valeur_fonciere'].notna()) & (df['valeur_fonciere'] != 0)]
  df = df[ (df['surface_reelle_bati'].notna()) & (df['surface_reelle_bati'] != 0)]
  df = df[ (df['longitude'].notna())]
  df = df[ (df['latitude'].notna()) ]
  df = df[ (df['nombre_lots'] == 0) ]
  df = df[ (df['code_departement'].notna()) ]
  return df

def group_by_data(df):
  col_gp = ['id_mutation', 'date_mutation', 'nature_mutation', 'code_postal', 'code_commune', 'code_region', 'code_departement',
            'type_local', 'latitude', 'longitude']
  return df.groupby(col_gp, as_index=False).agg( { 'valeur_fonciere': 'max', 'surface_reelle_bati': 'max', 'nombre_pieces_principales': 'max'})

def data_clean():
    output_data = ['id_mutation', 'date_mutation', 'nature_mutation',
                   'valeur_fonciere', 'code_postal', 'code_commune', 'code_region',
                   'code_departement', 'code_type_local', 'type_local',
                   'surface_reelle_bati', 'nombre_pieces_principales', 'latitude', 'longitude']
    years_files = ['2016', '2017', '2018', '2019', '2020']


    df_immo = []
    regions = read_file_region()
    for year in years_files:
        df = import_files_in_dict(year)
        df["code_departement"] = df["code_departement"].astype(str)
        df["code_departement"] = df["code_departement"].apply(lambda x: '0' + x if len(x) == 1 else x)
        df = pd.merge(df, regions, how='left', left_on='code_departement', right_on='code')
        df["code_region"] = df["region_code"]
        df_clean = filter_data(df)
        df_clean = df_clean[output_data]
        df_clean = group_by_data(df_clean)
        df_clean["prix_metre_carre"] = (df_clean['valeur_fonciere'] / df_clean['surface_reelle_bati']).apply(
            lambda x: round(x))
        df_clean = df_clean.sort_values(['code_region', 'latitude', 'longitude'])
        if len(df_immo) == 0:
            df_immo = df_clean
        else:
            df_immo = pd.concat([df_immo, df_clean])
    return df_immo

def data_prep(df_immo):
    # Appartement
    df_immo_appart = df_immo[(df_immo["nature_mutation"] == "Vente") & (df_immo["type_local"] == "Appartement")]

    # Maison
    df_immo_maison = df_immo[(df_immo["nature_mutation"] == "Vente") & (df_immo["type_local"] == "Maison")]

    return create_all_model_ball_tree_region(df_immo_maison)

def load_file_in_s3(name_file, path_aws):
    conn = boto3.session.Session(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    s3 = conn.client('s3')
    temp_file_path = './' + name_file
    if path.exists(temp_file_path):
        with open(temp_file_path, "rb") as f:
            s3.upload_fileobj(f, config.BUCKET_NAME, path_aws)

def download_file_s3(path_file_aws, name_file):
    conn = boto3.session.Session(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    s3 = conn.client('s3')
    temp_file_path = './' + name_file
    s3.download_file(config.BUCKET_NAME, path_file_aws, temp_file_path)


def create_all_model_ball_tree_region(df_immo_maison):
    df_immo_maison['distance_moyenne'] = np.zeros(len(df_immo_maison))
    df_immo_maison['index_voisins'] = np.zeros(len(df_immo_maison))
    models = {}
    regions = df_immo_maison.code_region.unique()
    df_prepare_ml = []
    for k in range(len(regions)):
        start = time.time()
        data = df_immo_maison[df_immo_maison.code_region == regions[k]]
        data = data.reset_index(drop=True)
        models[k] = BallTree(data[['latitude', 'longitude']].values, leaf_size=2, metric='haversine')

        name_file_model = "model_balltree_region_" + str(regions[k]) + ".p"
        pickle.dump(models[k], open("./model_balltree/" + name_file_model, "wb"))
        load_file_in_s3("model_balltree/" + name_file_model, "model_balltree/" + name_file_model)

        data = df_immo_maison[df_immo_maison.code_region == regions[k]]
        data = data.reset_index(drop=True)
        dist, indices = models[k].query(data[['latitude', 'longitude']].values, k=10)
        data['distance_moyenne'] = np.mean(dist[:, 1:] * 6341, 1)
        a = pd.DataFrame()
        a['prix_metre_carre'] = np.zeros(len(data))
        for i in range(1, 10):
            a += pd.DataFrame(data.iloc[indices[:, i], :]['prix_metre_carre']).reset_index(drop=True)
        a = a / 10
        data['prix_moyen_cartier'] = a.values
        data.sort_values(['id_mutation', 'latitude', 'longitude'])

        data.to_csv("./data-region/data_region_" + regions[k] + ".csv", index=False, header=True)
        load_file_in_s3("data-region/data_region_" + regions[k] + ".csv", "data-region/data_region_" + regions[k] + ".csv")

        if len(df_prepare_ml) == 0:
            df_prepare_ml = data
        else:
            df_prepare_ml = pd.concat([df_prepare_ml, data])
        stop = time.time()
        print(stop - start)

    return df_prepare_ml[['latitude', 'longitude', 'surface_reelle_bati', 'prix_metre_carre', 'code_region','prix_moyen_cartier']]

def execute_script_to_prepare_data():
    # Teléchargement des données
    download_files_etalab()
    # Telechargement du dataset des regions
    download_file_region()
    # Netoyyage des données
    df_immo = data_clean()
    # Preparation des données et creation des modeles balltree
    return data_prep(df_immo)
