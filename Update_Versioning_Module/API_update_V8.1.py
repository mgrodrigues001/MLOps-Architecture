import mlflow
import mlflow.sklearn
from flask import Flask, request, jsonify
import threading
import requests
from kafka import KafkaConsumer
import json
from multiprocessing import Process

# Declaracao das variaveis
contador = 0
taxa_load = 1000

lock = threading.Lock()

consumer = KafkaConsumer(
        'Atualizacao',
        bootstrap_servers= '10.32.2.213:32092',
        group_id='update',
        auto_offset_reset='latest',
        api_version=(0, 10, 1)
)

# Funcao de serializacao do modelo
def salva(modelo):
    global experiment_name, list_pods, experiment_id
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(nested=True):
        caminho = "/app/mlruns/" + experiment_id + "/" +str(mlflow.active_run().info.run_id) + "/artifacts/model"
        mlflow.sklearn.save_model(modelo, caminho, serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_CLOUDPICKLE)
        
        modelo_id = str(mlflow.active_run().info.run_id)
        for i in list_pods:
            r = requests.post(i,json=([experiment_id, modelo_id]))


app = Flask(__name__)

# Funcao principal
def update():
    global contador, model, taxa_load

    # carrega os dados da requisicao e separa em X e y
    for message in consumer:
        data = json.loads(message.value)
        Xi = (data[0])
        yi = (data[1])
        try:
            model = model.learn_one(Xi, yi)
        except:
            pass
        contador += 1

        if contador%taxa_load == 0:
            Process(target=salva, args=(model,)).start()

@app.route('/taxa', methods=['POST'])
def taxa():
    global taxa_load
    taxa_load = request.get_json(force=True)
    return jsonify(taxa_load)

@app.route('/cont', methods=['POST'])
def cont():
    global contador
    contador = request.get_json(force=True)
    return jsonify(contador)

@app.route('/pods', methods=['POST'])
def pods():
    global list_pods
    list_pods = request.get_json(force=True)
    return jsonify(list_pods)

@app.route('/load', methods=['POST'])
def load():
    global model, lock, experiment_name, experiment_id, list_pods

    experiment_name = request.get_json(force=True)

    with lock:
        mlflow.set_experiment(experiment_name)
        # Carrega o modelo
        runs_df = mlflow.search_runs(experiment_ids=[mlflow.get_experiment_by_name(experiment_name).experiment_id])
        first_run= runs_df.iloc[-1]
        experiment_id = str(first_run["experiment_id"])
        run_id = str(first_run["run_id"])
        model = mlflow.sklearn.load_model("/app/mlruns/" + experiment_id + "/" + run_id + "/artifacts/model")
        for i in list_pods:
            r = requests.post(i,json=([experiment_id, run_id]))
    return jsonify(f'{model}')

if __name__ == '__main__':
    threading.Thread(target=update).start()
    app.run(host='0.0.0.0', port=5002)