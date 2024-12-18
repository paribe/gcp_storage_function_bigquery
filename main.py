 import os
import csv
from google.cloud import storage, bigquery

# Configurações
PROJECT_ID = "stoked-virtue-321000"  # Substitua pelo seu ID do projeto
DATASET_ID = "clientes_dataset"
TABLE_ID = "tb_cliente"

def carregar_dados(event, context):
    """Função acionada pelo upload de um arquivo no Cloud Storage."""
    bucket_name = event['bucket']
    file_name = event['name']
    
    # Verifica se o arquivo é o cliente.csv
    if file_name != "cliente.csv":
        print(f"Arquivo {file_name} ignorado.")
        return

    client_storage = storage.Client()
    bucket = client_storage.bucket(bucket_name)
    blob = bucket.blob(file_name)
    dados = blob.download_as_text()

    linhas = []
    reader = csv.DictReader(dados.splitlines())
    for row in reader:
        linhas.append({
            "id_cliente": int(row["id_cliente"]),
            "nm_cliente": row["nm_cliente"],
            "end_cliente": row["end_cliente"],
            "cid_cliente": row["cid_cliente"],
            "est_cliente": row["est_cliente"],
            "pais_cliente": row["pais_cliente"]
        })

    # Insere os dados no BigQuery
    client_bq = bigquery.Client()
    tabela_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    errors = client_bq.insert_rows_json(tabela_ref, linhas)

    if errors:
        print(f"Erros ao inserir dados no BigQuery: {errors}")
    else:
        print(f"Dados do arquivo {file_name} carregados com sucesso.")    