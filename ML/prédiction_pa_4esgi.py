# -*- coding: utf-8 -*-
"""Prédiction PA-4ESGI.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1T_QO5QVek28isu072JMbbXQyUkSNRPGs
"""

from google.colab import drive
drive.mount('/content/drive')

import pandas as pd
import os

dataset_files = "/content/drive/MyDrive/data_pa/dataset-myimmo.csv"
df_immo = pd.read_csv(dataset_files)

# Appartement
df_immo_appart = df_immo[ (df_immo["nature_mutation"]=="Vente") & (df_immo["type_local"]=="Appartement") ]
# Maison
df_immo_maison = df_immo[ (df_immo["nature_mutation"]=="Vente") & (df_immo["type_local"]=="Maison") ]
df_immo_maison

"""Modele BallTree pour les appartements - 1 modéle par region"""

from sklearn.neighbors import BallTree
import numpy as np
import time

df_immo_maison['distance_moyenne']=np.zeros(len(df_immo_maison))
df_immo_maison['index_voisins']=np.zeros(len(df_immo_maison))
df_immo_maison

data_predict = pd.DataFrame()
data_predict['latitude'] = [42.958421]
data_predict['longitude'] = [9.452980]
data_predict['surface_reelle_bati'] = [84.0]
data_predict['code_region'] = [94]
df_immo_maison[df_immo_maison.code_region == data_predict.code_region[0]]

# Download model Balltree
from sklearn.externals import joblib

models = {}
regions = df_immo_maison.code_region.unique()
for k in range(len(regions)):
    start = time.time()
    name_model = 'model_balltree_region_' + str(regions[k]) + '.pkl'
    data = df_immo_maison[df_immo_maison.code_region==regions[k]]
    data = data.reset_index(drop=True)
    models = BallTree(data[['latitude', 'longitude']].values, leaf_size=2, metric='haversine')
    joblib_file = "/content/drive/MyDrive/data_pa/model_balltree/" + name_model
    joblib.dump(models, joblib_file)
    stop = time.time()
    print(stop-start)

data_predict = pd.DataFrame()
data_predict['latitude'] = [42.958421]
data_predict['longitude'] = [9.452980]
data_predict['surface_reelle_bati'] = [84.0]
data_predict['code_region'] = [94]

# load model balltree
from sklearn.externals import joblib
data = df_immo_maison[df_immo_maison.code_region == data_predict.code_region[0]]
data = data.reset_index(drop=True)
print(data[['latitude', 'longitude']])

joblib_file = "/content/drive/MyDrive/data_pa/model_balltree/model_balltree_region_"+str(data_predict.code_region[0])+".pkl"
model_load = joblib.load(joblib_file)

dist, indices = model_load.query(data_predict[['latitude','longitude']].values,k=10)
data_predict['distance_moyenne'] = np.mean(dist[:,1:]*6341,1)

print(dist)
print(indices)

a = pd.DataFrame()
a['prix_metre_carre'] = np.zeros(len(data_predict))
for i in range(1,10):
    a += pd.DataFrame(data.iloc[indices[:,i],:]['prix_metre_carre']).reset_index(drop=True)
a=a/10
data_predict['prix_moyen_cartier'] = a.values
data_predict

data = df_immo_maison[df_immo_maison.code_region == data_predict.code_region[0]]
data = data.reset_index(drop=True)
print(data[['latitude', 'longitude']])

models = BallTree(data[['latitude', 'longitude']].values, leaf_size=2, metric='haversine')
dist, indices = models.query(data_predict[['latitude','longitude']].values,k=10)

print(dist)
print(indices)

data_predict['distance_moyenne'] = np.mean(dist[:,1:]*6341,1)

a = pd.DataFrame()
a['prix_metre_carre'] = np.zeros(len(data_predict))
for i in range(1,10):
    a += pd.DataFrame(data.iloc[indices[:,i],:]['prix_metre_carre']).reset_index(drop=True)
a=a/10
data_predict['prix_moyen_cartier'] = a.values
data_predict

"""Prediction avec chargement des modéle"""



data_predict = pd.DataFrame()
data_predict['latitude'] = [42.958421]
data_predict['longitude'] = [9.452980]
data_predict['surface_reelle_bati'] = [84.0]
data_predict['code_region'] = [94]

# load model balltree
from sklearn.externals import joblib
data = df_immo_maison[df_immo_maison.code_region == data_predict.code_region[0]]
data = data.reset_index(drop=True)

joblib_file = "/content/drive/MyDrive/data_pa/model_balltree/model_balltree_region_"+str(data_predict.code_region[0])+".pkl"
model_load = joblib.load(joblib_file)

dist, indices = model_load.query(data_predict[['latitude','longitude']].values,k=10)
data_predict['distance_moyenne'] = np.mean(dist[:,1:]*6371)

print(dist)
print(indices)

a = pd.DataFrame()
a['prix_metre_carre'] = np.zeros(len(data_predict))
for i in range(1,10):
    a += pd.DataFrame(data.iloc[indices[:,i],:]['prix_metre_carre']).reset_index(drop=True)
a=a/10
data_predict['prix_moyen_cartier'] = a.values
data_predict

def getEncoderRegion(liste_region, code_region):
  for i in range(len(liste_region)):
    if liste_region[i] == code_region:
      return i
  return None

df_immo_maison.code_region.unique()

data_predict = data_predict[['latitude', 'longitude', 'surface_reelle_bati', 'code_region','prix_moyen_cartier']]
regions = df_immo_maison.code_region.unique()
encoder_region = getEncoderRegion(regions, data_predict.code_region[0])
print(encoder_region, data_predict.code_region[0])
data_predict['code_region'] = encoder_region
X = data_predict.values
X

from sklearn.externals import joblib
from sklearn.preprocessing import StandardScaler

# Path Model
joblib_file = "/content/drive/MyDrive/data_pa/model_immo_maison_full_region_7err.pkl"
# Load Model
joblib_model = joblib.load(joblib_file)
#predict
joblib_model.predict(X)

joblib_model.predict([ [ 15.848572, -61.644027,  98,         0,       18.8     ] ])
joblib_model.predict(X)