""" ------------------- Importacao Bibliotecas ------------------- """
import multiprocessing
import pandas as pd
import numpy as np
import time
import requests
import json
from sklearn.metrics import roc_auc_score
from confluent_kafka import Producer
from river import stream
import matplotlib.pyplot as plt
import csv
import os
import mlflow
import subprocess
import concurrent.futures
from multiprocessing import Pool
from multiprocessing import Process
import threading
import sys


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

    ["AGR_a_NB", "AGR_a.csv"],    
    ["AGR_a_ARF", "AGR_a.csv"],
    ["AGR_a_HT", "AGR_a.csv"],
    ["AGR_g_NB", "AGR_g.csv"],
    ["AGR_g_ARF", "AGR_g.csv"],
    ["AGR_g_HT", "AGR_g.csv"],
    ["youchoose_NB", "youchoose.csv"], 
    ["youchoose_ARF", "youchoose.csv"],
    ["youchoose_HT", "youchoose.csv"]
    
"""


experimentos = [
    ["AGR_a_HT", "AGR_a.csv"],
    ["AGR_g_HT", "AGR_g.csv"]
]


pod_name_result = subprocess.run(["kubectl -n kafka get pods --no-headers -o name | grep kafka"], capture_output=True, text=True, shell=True)
pod_name = pod_name_result.stdout.replace('\n', '')
print(pod_name)

for ex in experimentos:
    exp_name = ex[0]
    print(f'Experiment_Name: {exp_name}')

    """ ------------------- Declaracao de Variaveis ------------------- """
    df = pd.read_csv("datasets/csv/" + ex[1])
    name_df = ex[1]

    descricao = 'Teste de performance preditiva'
    n_processes = 6
    shape_df = df.shape
    atualiza_model_up = 1500 #  --> --> -->    FREQUENCIA DE VERSIONAMENTO DO MODELO    <-- <-- <-- <--

    manager = multiprocessing.Manager()
    df_queue = manager.Queue()
    [df_queue.put(record) for record in df.to_dict('records')]
        
    taxa_metrica = 1000
    tempo = manager.dict()
    metrica = manager.list()
    grafico = manager.list()
    list_lag = manager.list()

    lock1 = multiprocessing.Lock()
    lock2 = multiprocessing.Lock()

    kafka_bootstrap_servers = '10.32.2.213:32092'
    kafka_topic = 'Atualizacao'

    url = 'http://master:32001/predict'

    processes = []

    print(f'Frequencia de serializacao: {atualiza_model_up}')

    """ ------------------- Configura IPs do Cluster ------------------- """
    ips_load = []

    ips_out = subprocess.run(["kubectl get pods --no-headers -o custom-columns=':status.podIP' | grep 10.1.124"], capture_output=True, text=True, shell=True)
    ips = ips_out.stdout.split('\n')
    ips.remove('')
    num_pods_ativos = len(ips)
    for c in ips:
        ips_load.append(f'http://{c}:5001/load')

    url_pods = 'http://master:32002/pods'

    if num_pods_ativos == 1:
        r_pods = requests.post(url_pods,json=([ips_load[0]]))
    elif num_pods_ativos == 2:
        r_pods = requests.post(url_pods,json=(ips_load[0], ips_load[1]))
    elif num_pods_ativos == 3:
        r_pods = requests.post(url_pods,json=(ips_load[0], ips_load[1], ips_load[2]))
    elif num_pods_ativos == 4:
        r_pods = requests.post(url_pods,json=(ips_load[0], ips_load[1], ips_load[2], ips_load[3]))
    elif num_pods_ativos == 5:
        r_pods = requests.post(url_pods,json=(ips_load[0], ips_load[1], ips_load[2], ips_load[3], ips_load[4]))
    elif num_pods_ativos == 6:
        r_pods = requests.post(url_pods,json=(ips_load[0], ips_load[1], ips_load[2], ips_load[3], ips_load[4], ips_load[5]))
    elif num_pods_ativos == 7:
        r_pods = requests.post(url_pods,json=(ips_load[0], ips_load[1], ips_load[2], ips_load[3], ips_load[4], ips_load[5], ips_load[6]))
    elif num_pods_ativos == 8:
        r_pods = requests.post(url_pods,json=(ips_load[0], ips_load[1], ips_load[2], ips_load[3], ips_load[4], ips_load[5], ips_load[6], ips_load[7]))
    else:
        print('Erro ao configurar numero de pods')

    print(f'Numero de Pods ativos: {num_pods_ativos}')


    """ ------------------- Configura Experimento ------------------- """
    url_load = 'http://master:32002/load'
    r_load = requests.post(url_load, json=(ex[0]))


    """ ------------------- Reseta contador ------------------- """
    url_cont = 'http://master:32002/cont'
    r_cont = requests.post(url_cont, json=(0))


    """ ------------------- Configura taxa ------------------- """
    url_taxa = 'http://master:32002/taxa'
    r_taxa = requests.post(url_taxa, json=(atualiza_model_up))
    

    """ ------------------- Dados do Cluster ------------------- """
    #- Versao da API utilizada -#
    list_versao_infe = subprocess.run(["kubectl", "get", "deployment.apps/api-inferencia", "-o=jsonpath='{.spec.template.spec.containers[].image}'"], capture_output=True, text=True)
    versao_api_infe = list_versao_infe.stdout.split(':')[1][:-1]

    list_versao_up = subprocess.run(["kubectl", "get", "deployment.apps/api-update", "-o=jsonpath='{.spec.template.spec.containers[].image}'"], capture_output=True, text=True)
    versao_api_up = list_versao_up.stdout.split(':')[1][:-1]

    list_replicas_infe = subprocess.run(["kubectl", "get", "deployment.apps/api-inferencia", "-o=jsonpath='{.spec.replicas}'"], capture_output=True, text=True)
    n_replicas_infe = int(list_replicas_infe.stdout[1])

    list_replicas_up = subprocess.run(["kubectl", "get", "deployment.apps/api-update", "-o=jsonpath='{.spec.replicas}'"], capture_output=True, text=True)
    n_replicas_up = int(list_replicas_up.stdout[1])

    list_cpu_infe = subprocess.run(["kubectl", "get", "deployment.apps/api-inferencia", "-o=jsonpath='{.spec.template.spec.containers[].resources.limits.cpu}'"], capture_output=True, text=True)
    CPU_limit_infe = list_cpu_infe.stdout

    list_cpu_up = subprocess.run(["kubectl", "get", "deployment.apps/api-update", "-o=jsonpath='{.spec.template.spec.containers[].resources.limits.cpu}'"], capture_output=True, text=True)
    CPU_limit_up = list_cpu_up.stdout


    """ ------------------- Declaracao das Funcoes ------------------- """
    def get_lag(list_lag):
        global pod_name
        try:
            kubectl_command = ["kubectl", "exec", "-it", pod_name, "--namespace=kafka", "--", "kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092", "--describe", "--group", "update"]
            kubectl_output = subprocess.run(kubectl_command, capture_output=True, text=True)
            awk_command = ['awk', 'NR > 1 {sum += $6} END {print sum}']
            lag = subprocess.run(awk_command, input=kubectl_output.stdout, capture_output=True, text=True)
            list_lag.append(int(lag.stdout.strip()))
        except:
            list_lag.append(0)

    def desempenho(metrica, grafico):
        global lock2
        y_true_list = []
        y_score_list = []

        with lock2:
            for i in metrica[-taxa_metrica:]:
                y_true_list.append(i[0])
                y_score_list.append(i[1])

            y_true = np.array(y_true_list)
            y_score = np.array(y_score_list)
            grafico.append(round(roc_auc_score(y_true, y_score), 4))
            #print(f'Len Grafico: {len(grafico)}')

    def inf(t, lista, tempo, metrica, grafico, list_lag):
        global taxa_metrica, kafka_bootstrap_servers, kafka_topic
        producer = Producer({'bootstrap.servers': kafka_bootstrap_servers, "queue.buffering.max.messages": 100000000})
        while not lista.empty():
            try:
                X = lista.get()
                y = X.popitem()
                r = s.post(url, json=(X))
                metrica.append([y[1], r.json()])

                if len(metrica) % taxa_metrica == 0:
                    desempenho(metrica, grafico)
                    threading.Thread(target=get_lag, args=(list_lag,)).start()

                json_message = json.dumps([X, y[1]])
                producer.produce(kafka_topic, key=None, value=json_message.encode('utf-8'))
            except multiprocessing.TimeoutError:
                pass  # A fila está vazia, então continue
            except:
                break
            #print(f'Len Lista: {lista.qsize()}')
        producer.flush()
        tempo[t] = (time.time())


    """ ------------------- Inicio do Processo ------------------- """
    print(f'Executando Experimento {exp_name}...')

    s = requests.Session()

    tempo_inicial = time.time()

    for i in range(n_processes):
        process = multiprocessing.Process(target=inf, args=(f't{i+1}', df_queue, tempo, metrica, grafico, list_lag))
        processes.append(process)
        process.start()

        # Aguarda até que todos os processos terminem ou a fila esteja vazia
    #while any(process.is_alive() for process in processes) or not df_queue.empty():
    while not df_queue.empty():
        time.sleep(2)
        
    time.sleep(1)

    for process in processes:
        process.terminate()  # Termina os processos manualmente

    s.close()
    
    """ ------------------- Verificando condicao da fila kafka ------------------- """
    print('Verificando tempo restante pra consumir fila')
    kubectl_command = ["kubectl", "exec", "-it", pod_name, "--namespace=kafka", "--", "kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092", "--describe", "--group", "update"]
    kubectl_output = subprocess.run(kubectl_command, capture_output=True, text=True)
    awk_command = ['awk', 'NR > 1 {sum += $6} END {print sum}']
    lag = subprocess.run(awk_command, input=kubectl_output.stdout, capture_output=True, text=True)
    cond_fila = int(lag.stdout.strip())
    while cond_fila != 0:
        kubectl_output = subprocess.run(kubectl_command, capture_output=True, text=True)
        awk_command = ['awk', 'NR > 1 {sum += $6} END {print sum}']
        lag = subprocess.run(awk_command, input=kubectl_output.stdout, capture_output=True, text=True)
        cond_fila = int(lag.stdout.strip())    
        if cond_fila > 20000:
            time.sleep(40)
        else:
            time.sleep(10)

    fim_tempo_geral = time.time()


    """ ------------------- Prepara logs para gravacao ------------------- """
    n_df,_ = name_df.split('.')
    n_total_instancias = len(metrica)
    tempo_gravacao = {}
    lista_tempos = []

    for i in tempo:
        tempo_gravacao[i] = (tempo[i] - tempo_inicial)
        lista_tempos.append(tempo[i] - tempo_inicial)

    media_tempo = round(sum(lista_tempos)/len(lista_tempos), 4)
    tempo_cons_geral = round(fim_tempo_geral-tempo_inicial, 4)

    # Exp_Performance_Fila.csv
    path = 'arq_logs/logs_exp_preform/Exp_Perf_Preditiva_Por_Endpoints_2.csv'
    isFile = os.path.isfile(path)

    if isFile:
        cont_linhas = subprocess.run(["wc", "-l", "arq_logs/logs_exp_preform/Exp_Perf_Preditiva_Por_Endpoints_2.csv"], capture_output=True, text=True)
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
        "n_endpoints_infe",
        "n_endpoints_up",
        "limite_cpu_infe",
        "limite_cpu_up",
        "shape_df",
        "n_processes",
        "n_total_instancias",
        "freq_atualiza_model",
        "Evolucao_metrica",
        "fila_kafka",
        "tempos_infe_process",
        "media_tempo_infe",
        "tempo_consumo_geral",
        "descricao"
    ]

    logs = [
        f'{id_linhas}',
        versao_api_infe,
        versao_api_up,
        exp_name,
        name_df,
        n_replicas_infe,
        n_replicas_up,
        CPU_limit_infe,
        CPU_limit_up,
        shape_df,
        n_processes,
        n_total_instancias,
        atualiza_model_up,
        grafico,
        list_lag,
        tempo_gravacao,
        media_tempo,
        tempo_cons_geral,
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
