""" ------------------- Importacao Bibliotecas ------------------- """
import pandas as pd
import numpy as np
import time
from sklearn.metrics import roc_auc_score
from confluent_kafka import Producer
from river import stream
import csv
import os
import mlflow
import subprocess


""" 
    ["AGR_a_NB", "AGR_a.csv"],
    ["AGR_a_ARF", "AGR_a.csv"],
    ["AGR_a_HT", "AGR_a.csv"],
    ["AGR_a_SRP", "AGR_a.csv"],
    ["AGR_g_NB", "AGR_g.csv"],
    ["AGR_g_ARF", "AGR_g.csv"],
    ["AGR_g_HT", "AGR_g.csv"],
    ["AGR_g_SRP", "AGR_g.csv"],
    ["airlines_NB", "airlines.csv"],
    ["airlines_ARF", "airlines.csv"],
    ["airlines_HT", "airlines.csv"],
    ["airlines_SRP", "airlines.csv"],
    ["youchoose_NB", "youchoose.csv"], 
    ["youchoose_ARF", "youchoose.csv"],
    ["youchoose_HT", "youchoose.csv"],
    ["youchoose_SRP", "youchoose.csv"]  
"""


experimentos = [
    ["AGR_a_NB", "AGR_a.csv"],
    ["AGR_a_ARF", "AGR_a.csv"],
    ["AGR_a_HT", "AGR_a.csv"],
    ["AGR_a_SRP", "AGR_a.csv"],
    ["AGR_g_NB", "AGR_g.csv"],
    ["AGR_g_ARF", "AGR_g.csv"],
    ["AGR_g_HT", "AGR_g.csv"],
    ["AGR_g_SRP", "AGR_g.csv"],
    ["airlines_NB", "airlines.csv"],
    ["airlines_ARF", "airlines.csv"],
    ["airlines_HT", "airlines.csv"],
    ["airlines_SRP", "airlines.csv"],
    ["youchoose_NB", "youchoose.csv"], 
    ["youchoose_ARF", "youchoose.csv"],
    ["youchoose_HT", "youchoose.csv"],
    ["youchoose_SRP", "youchoose.csv"]
]


for ex in experimentos:
    exp_name = ex[0]
    print(f'Experiment_Name: {exp_name}')

    """ ------------------- Declaracao de Variaveis ------------------- """
    df = pd.read_csv("datasets/csv/" + ex[1])
    name_df = ex[1]
    descricao = 'Baseline Atualizacao Incremental'
    n_processes = 1
    shape_df = df.shape
    atualiza_model_up = 1
    taxa_metrica = 1000


    """ ------------------- Carrega o Modelo ------------------- """
    mlflow.set_experiment(exp_name)
    runs_df = mlflow.search_runs(experiment_ids=[mlflow.get_experiment_by_name(exp_name).experiment_id])
    first_run= runs_df.iloc[-1]
    experiment_id = str(first_run["experiment_id"])
    run_id = str(first_run["run_id"])
    model = mlflow.sklearn.load_model("mlruns/" + experiment_id + "/" + run_id + "/artifacts/model")

    """ ------------------- Coleta Tamanho Inicial do Modelo ------------------- """
    model_path = "mlruns/" + experiment_id + "/" + run_id + "/artifacts/model/model.pkl"
    tamanho_modelo_bytes_init = os.path.getsize(model_path)
    size_model_init = round(tamanho_modelo_bytes_init/1024, 2)

    """ ------------------- Separando DataFrame em X e y ------------------- """
    target = df.columns[-1]
    X = pd.DataFrame(df)
    y = X.pop(target)


    """ ------------------- Processo de inferencia e atualizacao ------------------- """
    grafico = []
    metrica = []
    t_ini = time.time()
    for xi, yi in stream.iter_pandas(X, y):
        y_pred = model.predict_proba_one(xi)
        metrica.append([yi, y_pred[1]])
        model = model.learn_one(xi, yi)
        if len(metrica) % 1000 == 0:
            y_true_list = []
            y_score_list = []
            for i in metrica[-1000:]:
                y_true_list.append(i[0])
                y_score_list.append(i[1])

            y_true = np.array(y_true_list)
            y_score = np.array(y_score_list)
            grafico.append(round(roc_auc_score(y_true, y_score), 4))
    t_fim = time.time()
    tempo = round(t_fim - t_ini, 4)


    """ ------------------- Coleta Tamanho Final do Modelo ------------------- """
    mlflow.set_experiment(exp_name)
    with mlflow.start_run(nested=True):
        caminho = "/app/mlruns/" + experiment_id + "/" +str(mlflow.active_run().info.run_id) + "/artifacts/model"
        mlflow.sklearn.save_model(model, caminho, serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_CLOUDPICKLE)

    time.sleep(2)
    tamanho_modelo_bytes_fin = os.path.getsize(caminho)
    size_model_fin = round(tamanho_modelo_bytes_fin/1024, 2)


    """ ------------------- Prepara logs para gravacao ------------------- """
    n_df,_ = name_df.split('.')
    n_total_instancias = len(metrica)
    tempo_gravacao = tempo
    media_tempo = tempo

    path = 'arq_logs/logs_exp_baseline/Exp_baseline_temp_ini_fin.csv'
    isFile = os.path.isfile(path)

    if isFile:
        cont_linhas = subprocess.run(["wc", "-l", "arq_logs/logs_exp_baseline/Exp_baseline_temp_ini_fin.csv"], capture_output=True, text=True)
        n_linhas,_ = cont_linhas.stdout.split(' ')
        id_linhas = f'Exp-{n_linhas}'
    else:
        id_linhas = 'Exp-1'


    """ ------------------- Criação das Colunas ------------------- """
    cabecalho = [
        "id",
        "versao_api_infe",
        "versao_api_up",
        "exp_name",
        "name_df",
        "n_replicas_infe",
        "n_replicas_up",
        "limite_cpu_infe",
        "limite_cpu_up",
        "shape_df",
        "n_processes",
        "n_total_instancias",
        "freq_atualiza_model",
        "Evolucao_metrica",
        "fila_lag",
        "tempos_Processes",
        "media_tempo_infe",
        "tamanho_model_init",
        "tamanho_model_fin",
        "descricao"
    ]

    logs = [
        f'{id_linhas}',
        'N/A',
        'N/A',
        exp_name,
        name_df,
        0,
        0,
        'N/A',
        'N/A',
        shape_df,
        n_processes,
        n_total_instancias,
        atualiza_model_up,
        grafico,
        'N/A',
        tempo_gravacao,
        media_tempo,
        size_model_init,
        size_model_fin,
        descricao
    ]


    """ ------------------- Gravacao dos log dos experimentos ------------------- """
    if isFile:
        with open(path, 'a', newline='', encoding='utf-8') as f:
            w = csv.writer(f, delimiter=';')
            w.writerow(logs)
    else:
        with open(path, 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f, delimiter=';')
            w.writerow(cabecalho)
            w.writerow(logs)
    f.close()
    print(f'Gravacao de {id_linhas} OK')
    print(60*'_')

    time.sleep(2)
