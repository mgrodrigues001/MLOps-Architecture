import mlflow
import mlflow.sklearn
from flask import Flask, request, jsonify
import threading

lock = threading.Lock()

app = Flask(__name__)

@app.route('/predict', methods=['POST'])
def predict():
    global model

    data = request.get_json(force=True)

    y_pred = model.predict_proba_one(data)

    return jsonify(y_pred[1])

@app.route('/load', methods=['POST'])
def load():
    global model, lock

    experment = request.get_json(force=True)
    with lock:
        model = mlflow.sklearn.load_model("/app/mlruns/" + str(experment[0]) + "/" + str(experment[1]) + "/artifacts/model")

    return jsonify(f'{model}')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)