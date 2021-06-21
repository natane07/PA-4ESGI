from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import numpy as np
import pandas as pd
import pickle
import boto3
from os import path
import config

def train_model(df_ml):
    df_ml = df_ml[['latitude', 'longitude', 'surface_reelle_bati', 'prix_metre_carre', 'code_region', 'prix_moyen_cartier']]

    y = df_ml['prix_metre_carre'].values
    X = df_ml.drop(columns=['prix_metre_carre']).values

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,random_state=12)
    forest = RandomForestRegressor(n_estimators=40,min_samples_split=5)
    forest.fit(X_train,y_train)
    shema = pd.DataFrame((np.abs(y_test - forest.predict(X_test)) / y_test)) * 100
    shema[shema < 200].hist(bins=1000)
    err_model = shema.iloc[0].astype(float).median()
    print("% d'erreur modele: ", err_model)

    name_file_model = "pickled_model_" + str(round(err_model, 2)) + ".p"
    pickle.dump(forest, open("./" + name_file_model, "wb"))
    load_file_in_s3(name_file_model, name_file_model)


def load_file_in_s3(name_file, path_aws):
    conn = boto3.session.Session(config.AWS_ACCESS_KEY, config.AWS_SECRET_KEY)
    s3 = conn.client('s3')
    temp_file_path = './' + name_file
    if path.exists(temp_file_path):
        with open(temp_file_path, "rb") as f:
            s3.upload_fileobj(f, config.BUCKET_NAME, path_aws)