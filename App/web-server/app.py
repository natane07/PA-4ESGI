from flask import Flask, request, json
import predict_ml
import data_prep
import ml
import predict_ml
import data_vis
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/data_visualition')
def execute_data_visualisation():
    data_vis.data_vis_execute()
    return "Data visualition generate"

@app.route('/execute_script')
def execute_script_data_and_ml():
    df_ml = data_prep.execute_script_to_prepare_data()
    ml.train_model(df_ml)
    return "success script execute"

# @app.route('/predict')
@app.route('/predict', methods = ['POST'])
def predict():
    # Parse request
    body_dict = request.get_json(silent=True)
    # Preparation des données pour la prediction
    data = predict_ml.prepare_data(body_dict)

    # Donnée pour la prediction
    latitude = str(body_dict['latitude'])
    longitude = str(body_dict['longitude'])
    surface_reelle_bati = str(body_dict['surface_reelle_bati'])
    code_region = data["code_region"].values[0]
    prix_moyen_cartier = data["prix_moyen_cartier"].values[0]
    distance_moyenne = data["distance_moyenne"].values[0]
    data = [str(latitude), str(longitude), str(surface_reelle_bati), str(code_region), str(prix_moyen_cartier)]

    # Prediction du modèle
    result = predict_ml.predict_model(data)

    # Json
    json_reponse = {
        'prediction': result,
        'latitude': latitude,
        'longitude': longitude,
        'surface_reelle_bati': surface_reelle_bati,
        'code_region': code_region,
        'prix_moyen_cartier':  str(round(prix_moyen_cartier, 2)),
        'distance_moyenne': str(round(distance_moyenne, 2)),
    }

    return json.dumps(json_reponse)

if __name__ == '__main__':
    app.run()
