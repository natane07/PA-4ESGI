import data_prep
import urllib.request
import pandas as pd
from os import path
import matplotlib.pyplot as plt
import boto3
import config
import datetime

def load_file_public_in_s3(name_file, path_aws):
    """
    Upload d'un fichier sur S3
    :param name_file: Nom du fichier
    :param path_aws: Chemin du fichier
    :return: None
    """
    # Ouverture de la session S3
    conn = boto3.session.Session(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    s3 = conn.client('s3')
    temp_file_path = './' + name_file
    # Upload du fichier dans S3
    if path.exists(temp_file_path):
        with open(temp_file_path, "rb") as f:
            s3.upload_fileobj(f, config.BUCKET_NAME, path_aws, ExtraArgs={'ACL':'public-read'})

def download_files_etalab():
    """
    Téléchargement des données brut depuis data-gouv
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
        if not path.exists(path_file):
            urllib.request.urlretrieve(url_files, path_file)

def group_by_data(df):
    """
    Regroupement des données qui sont divisé
    :param df: le dataframe des données
    :return: le dataframe des données regroupé
    """
    col_gp = ['id_mutation', 'date_mutation', 'nature_mutation', 'code_postal', 'code_commune', 'code_region', 'code_departement', 'name',
            'type_local']
    return df.groupby(col_gp, as_index=False).agg( { 'valeur_fonciere': 'max', 'surface_reelle_bati': 'max', 'nombre_pieces_principales': 'max'})

def data_clean():
    """
    Nétoyage et traitement des données
    :return: les données traité et clean
    """
    # Les variables à récuperer
    output_data = ['id_mutation', 'date_mutation', 'nature_mutation',
                   'valeur_fonciere', 'code_postal', 'code_commune', 'code_region',
                   'code_departement', 'name', 'code_type_local', 'type_local',
                   'surface_reelle_bati', 'nombre_pieces_principales']
    # Création du tableau des 5 dernieres années courantes
    date = datetime.datetime.now()
    yearNow = date.year
    years_files = []
    for i in range(1, 6):
        year = str(yearNow - i)
        years_files.append(year)

    df_immo = []
    # Teléchargement du dataset des départements
    regions = data_prep.read_file_region()
    # Traitement des données pour chaque années (les 5 dernieres années)
    for year in years_files:
        df = data_prep.import_files_in_dict(year)
        # transformation du code departement pour le merge avec l'autre dataset
        df["code_departement"] = df["code_departement"].astype(str)
        df["code_departement"] = df["code_departement"].apply(lambda x: '0' + x if len(x) == 1 else x)
        df = pd.merge(df, regions, how='left', left_on='code_departement', right_on='code')
        df["code_region"] = df["region_code"]
        # filtrage des données
        df_clean = data_prep.filter_data(df)
        # recupération des variables intéressantes
        df_clean = df_clean[output_data]
        # regroupement des données
        df_clean = group_by_data(df_clean)
        # Création de la variable prix_metre_carré par bien
        df_clean["prix_metre_carre"] = (df_clean['valeur_fonciere'] / df_clean['surface_reelle_bati']).apply(lambda x: round(x))
        df_clean = df_clean.sort_values(['code_region'])
        # Concaténation des données de chaque année
        if len(df_immo) == 0:
            df_immo = df_clean
        else:
            df_immo = pd.concat([df_immo, df_clean])
    df_immo = df_immo[(df_immo["nature_mutation"] == "Vente") & ((df_immo["type_local"] == "Appartement") | (df_immo["type_local"] == "Maison"))]
    return df_immo

def data_region():
    """
    Création du dataframe avec le code de la region et son nom
    :return: le dataframe des régions
    """
    code_regions = ["11", "24", "27", "28", "32", "44", "52", "53", "75", "76", "84", "93", "94"]
    name_regions = ["Ile-de-France", "Centre-Val-de-Loire", "Bourgogne-Franche-Comte", "Normandie", "Hauts-de-France",
                    "Grand-Est", "Pays-de-la-Loire", "Bretagne", "Nouvelle-Aquitaine", "Occitanie",
                    "Auvergne-Rhone-Alpes", "Provence-Alpes-Cote-d-Azur", "Corse"]
    df_name_regions = pd.DataFrame(code_regions)
    df_name_regions['code_regions'] = code_regions
    df_name_regions["code_regions"] = df_name_regions["code_regions"].apply(lambda x: '0' + x if len(x) == 1 else x)
    df_name_regions['name_regions'] = name_regions
    return df_name_regions

def moyenne_val_fonciere_region(df_immo_region):
    """
    Génération des graphiques pour récuperer la moyenne de la valeur fonciere pour chaque région
    :param df_immo_region: la dataset des données
    :return: None
    """
    # traitement des données
    col_gp = ['code_region', 'name_regions']
    df_valeur_fonciere_regions = df_immo_region.groupby(col_gp, as_index=False).agg({'valeur_fonciere': 'mean'})
    df_valeur_fonciere_regions['valeur_fonciere'] = round(df_valeur_fonciere_regions['valeur_fonciere'], 2)
    df_valeur_fonciere_regions['valeur_fonciere'] = df_valeur_fonciere_regions["valeur_fonciere"].apply(
        lambda x: float(x))
    df_valeur_fonciere_regions = df_valeur_fonciere_regions.sort_values('valeur_fonciere')
    # Création du graphique
    ax = df_valeur_fonciere_regions.plot(x="name_regions", y="valeur_fonciere", figsize=(12, 12), kind='bar',
                                    title="Moyenne des valeurs fonciere par region")
    ax.get_legend().remove()
    fname = './data-vis/moyenne_des_valeurs_fonciere_par_region.png'
    # Sauvegarde du graphique
    plt.savefig(fname, dpi=72, bbox_inches='tight')
    path_aws = 'data-vis/moyenne_des_valeurs_fonciere_par_region.png'
    # Upload du graphique sur S3
    load_file_public_in_s3(path_aws, path_aws)

def moyenne_mcarre_bien_region(df_immo_region):
    """
    Génération des graphiques pour récuperer la moyenne du metre carré des biens immobiliers de chaque région
    :param df_immo_region: la dataset des données
    :return: None
    """
    # traitement des données
    col_gp = ['code_region', 'name_regions']
    df_immo_region_filter = df_immo_region[(df_immo_region["prix_metre_carre"] <= 50000)]
    df_prix_metre_carre_regions = df_immo_region_filter.groupby(col_gp, as_index=False).agg({'prix_metre_carre': 'mean'})
    df_prix_metre_carre_regions['prix_metre_carre'] = round(df_prix_metre_carre_regions['prix_metre_carre'], 2)
    df_prix_metre_carre_regions['prix_metre_carre'] = df_prix_metre_carre_regions["prix_metre_carre"].apply(
        lambda x: float(x))
    df_prix_metre_carre_regions = df_prix_metre_carre_regions.sort_values('prix_metre_carre')
    # Création du graphique
    ax = df_prix_metre_carre_regions.plot(x="name_regions", y="prix_metre_carre", figsize=(12, 12), kind='bar',
                                     title="Moyenne du m2 d'un bien par region")
    ax.get_legend().remove()
    fname = './data-vis/moyenne_du_prix_m2_par_region.png'
    # Sauvegarde du graphique
    plt.savefig(fname, dpi=72, bbox_inches='tight')
    path_aws = 'data-vis/moyenne_du_prix_m2_par_region.png'
    # Upload du graphique sur S3
    load_file_public_in_s3(path_aws, path_aws)

def moyenne_surface_bien_region(df_immo_region):
    """
    Génération des graphiques pour récuperer la moyenne de la surafce des biens immobiliers de chaque région
    :param df_immo_region: la dataset des données
    :return: None
    """
    # traitement des données
    col_gp = ['code_region', 'name_regions']
    df_surface_reelle_bati_regions = df_immo_region.groupby(col_gp, as_index=False).agg({'surface_reelle_bati': 'mean'})
    df_surface_reelle_bati_regions['surface_reelle_bati'] = round(df_surface_reelle_bati_regions['surface_reelle_bati'], 2)
    df_surface_reelle_bati_regions['surface_reelle_bati'] = df_surface_reelle_bati_regions["surface_reelle_bati"].apply(lambda x: float(x))
    df_surface_reelle_bati_regions = df_surface_reelle_bati_regions.sort_values('surface_reelle_bati')
    # Création du graphique
    ax = df_surface_reelle_bati_regions.plot(x="name_regions", y="surface_reelle_bati", figsize=(12, 12),
                                        kind='bar', title="Moyenne de la surface d'un bien par region")
    ax.get_legend().remove()
    fname = './data-vis/moyenne_des_surfaces_bien_par_region.png'
    # Sauvegarde du graphique
    plt.savefig(fname, dpi=72, bbox_inches='tight')
    path_aws = 'data-vis/moyenne_des_surfaces_bien_par_region.png'
    # Upload du graphique sur S3
    load_file_public_in_s3(path_aws, path_aws)

def moyenne_val_fonciere_departement(df_immo_region):
    """
    Génération des graphiques pour récuperer la moyenne de la valeur fonciere pour chaque département de chaque région
    :param df_immo_region: la dataset des données
    :return: None
    """
    # code des regions de france
    code_regions = ["11", "24", "27", "28", "32", "44", "52", "53", "75", "76", "84", "93", "94"]
    col_gp = ['code_region', 'name_regions', 'name']
    # traitement des données
    df_valeur_fonciere_regions = df_immo_region.groupby(col_gp, as_index=False).agg({'valeur_fonciere': 'mean'})
    df_valeur_fonciere_regions['valeur_fonciere'] = round(df_valeur_fonciere_regions['valeur_fonciere'], 2)
    df_valeur_fonciere_regions['valeur_fonciere'] = df_valeur_fonciere_regions["valeur_fonciere"].apply(lambda x: float(x))
    df_valeur_fonciere_regions = df_valeur_fonciere_regions.sort_values('valeur_fonciere')
    # Traitement pour chaque region
    for code in code_regions:
        # filtrage des données sur la regions
        df_reg_dep = df_valeur_fonciere_regions[df_valeur_fonciere_regions['code_region'] == code]
        if len(df_reg_dep['name_regions'].values) != 0:
            # Création du graphique
            title = "Moyenne des valeurs fonciere en " + df_reg_dep['name_regions'].values[0]
            fname = "./data-vis/moyenne_des_valeurs_fonciere_" + df_reg_dep['name_regions'].values[0] + '.png'
            ax = df_reg_dep.plot(x="name", y="valeur_fonciere", figsize=(12, 12), kind='bar', title=title)
            ax.get_legend().remove()
            _nb = 0
            # Ajout d'une anotation en haute de chaque graphe en bar
            for i in df_reg_dep["valeur_fonciere"].values:
                pct = i
                pct = round(pct, 2)
                ax.annotate(str(pct), xy=(_nb, i + 50), ha='center', va='bottom')
                _nb += 1
            # Sauvegarde du graphique
            plt.savefig(fname, dpi=72, bbox_inches='tight')
            # Upload du graphique sur S3
            path_aws = "data-vis/moyenne_des_valeurs_fonciere_" + df_reg_dep['name_regions'].values[0] + '.png'
            load_file_public_in_s3(path_aws, path_aws)

def moyenne_mcarre_bien_departement(df_immo_region):
    """
    Génération des graphiques pour récuperer la moyenne du metre carré des biens immobiliers de chaque département pour chaque région
    :param df_immo_region: la dataset des données
    :return: None
    """
    # code des regions de france
    code_regions = ["11", "24", "27", "28", "32", "44", "52", "53", "75", "76", "84", "93", "94"]
    col_gp = ['code_region', 'name_regions', 'name']
    # traitement des données
    df_immo_region_filter = df_immo_region[(df_immo_region["prix_metre_carre"] <= 50000)]
    df_prix_metre_carre_regions = df_immo_region_filter.groupby(col_gp, as_index=False).agg({'prix_metre_carre': 'mean'})
    df_prix_metre_carre_regions['prix_metre_carre'] = round(df_prix_metre_carre_regions['prix_metre_carre'], 2)
    df_prix_metre_carre_regions['prix_metre_carre'] = df_prix_metre_carre_regions["prix_metre_carre"].apply(
        lambda x: float(x))
    df_prix_metre_carre_regions = df_prix_metre_carre_regions.sort_values('prix_metre_carre')
    # Traitement pour chaque region
    for code in code_regions:
        # filtrage des données sur la regions
        df_reg_dep = df_prix_metre_carre_regions[df_prix_metre_carre_regions['code_region'] == code]
        if len(df_reg_dep['name_regions'].values) != 0:
            title = "Moyenne du m2 d'un bien en " + df_reg_dep['name_regions'].values[0]
            fname = "./data-vis/moyenne_du_prix_m2_" + df_reg_dep['name_regions'].values[0] + '.png'
            # Création du graphique
            ax = df_reg_dep.plot(x="name", y="prix_metre_carre", figsize=(12, 12), kind='bar', title=title)
            ax.get_legend().remove()
            _nb = 0
            # Ajout d'une anotation en haute de chaque graphe en bar
            for i in df_reg_dep["prix_metre_carre"].values:
                pct = i
                pct = round(pct, 2)
                ax.annotate(str(pct), xy=(_nb, i + 50), ha='center', va='bottom')
                _nb += 1
            # Sauvegarde du graphique
            plt.savefig(fname, dpi=72, bbox_inches='tight')
            # Upload du graphique sur S3
            path_aws = "data-vis/moyenne_du_prix_m2_" + df_reg_dep['name_regions'].values[0] + '.png'
            load_file_public_in_s3(path_aws, path_aws)

def moyenne_surface_bien_departement(df_immo_region):
    """
    Génération des graphiques pour récuperer la moyenne de la surafce des biens immobiliers de chaque département pour chaque région
    :param df_immo_region: la dataset des données
    :return: None
    """
    # code des regions de france
    code_regions = ["11", "24", "27", "28", "32", "44", "52", "53", "75", "76", "84", "93", "94"]
    col_gp = ['code_region', 'name_regions', 'name']
    # traitement des données
    df_surface_bati_regions = df_immo_region.groupby(col_gp, as_index=False).agg({'surface_reelle_bati': 'mean'})
    df_surface_bati_regions['surface_reelle_bati'] = round(df_surface_bati_regions['surface_reelle_bati'], 2)
    df_surface_bati_regions['surface_reelle_bati'] = df_surface_bati_regions["surface_reelle_bati"].apply(lambda x: float(x))
    df_surface_bati_regions = df_surface_bati_regions.sort_values('surface_reelle_bati')
    # Traitement pour chaque region
    for code in code_regions:
        # filtrage des données sur la regions
        df_reg_dep_surf = df_surface_bati_regions[df_surface_bati_regions['code_region'] == code]
        if len(df_reg_dep_surf['name_regions'].values) != 0:
            # Création du graphique
            title = "Moyenne de la surface d'un bien en " + df_reg_dep_surf['name_regions'].values[0]
            fname = "./data-vis/moyenne_des_surface_bien_" + df_reg_dep_surf['name_regions'].values[0] + '.png'
            ax = df_reg_dep_surf.plot(x="name", y="surface_reelle_bati", figsize=(12, 12), kind='bar', title=title)
            ax.get_legend().remove()
            _nb = 0
            # Ajout d'une anotation en haute de chaque graphe en bar
            for i in df_reg_dep_surf["surface_reelle_bati"].values:
                pct = i
                pct = round(pct, 2)
                ax.annotate(str(pct), xy=(_nb, i), ha='center', va='bottom')
                _nb += 1
            # Sauvegarde du graphique
            plt.savefig(fname, dpi=72, bbox_inches='tight')
            # Upload du graphique sur S3
            path_aws = "data-vis/moyenne_des_surface_bien_" + df_reg_dep_surf['name_regions'].values[0] + '.png'
            load_file_public_in_s3(path_aws, path_aws)

def data_vis_execute():
    # Téléchargement des données depuis data-gouv
    download_files_etalab()
    # Clean et traitement des données
    df_immo = data_clean()
    # Dataset des regions avec leurs noms
    df_name_regions = data_region()
    # merge des dataset
    df_immo_region = pd.merge(df_immo, df_name_regions, how='left', left_on='code_region', right_on='code_regions')
    # génération des graphique pour chaque restitution
    moyenne_val_fonciere_region(df_immo_region)
    moyenne_val_fonciere_departement(df_immo_region)
    moyenne_mcarre_bien_region(df_immo_region)
    moyenne_mcarre_bien_departement(df_immo_region)
    moyenne_surface_bien_region(df_immo_region)
    moyenne_surface_bien_departement(df_immo_region)

