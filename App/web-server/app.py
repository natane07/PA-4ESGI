from flask import Flask, request, json
import predict_ml
import data_prep
import ml
import predict_ml

app = Flask(__name__)


@app.route('/execute_script')
def execute_script_data_and_ml():
    df_ml = data_prep.execute_script_to_prepare_data()
    ml.train_model(df_ml)
    return "success script execute"

# @app.route('/predict')
@app.route('/predict', methods = ['POST'])
def predict():
    # Parse request body for model input
    # body_dict = request.get_json(silent=True)
    # data = body_dict['data']
    # input = float(data)

    data = [15.848572, -61.644027, 98, 0, 18.8]
    result = predict_ml.predict_model(data)
    json_reponse = { 'prediction': result }

    return json.dumps(json_reponse)


if __name__ == '__main__':
    app.run()
