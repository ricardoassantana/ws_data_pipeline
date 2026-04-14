import os
import pandas as pd
from datetime import datetime 
from airflow.sdk import dag, task
from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

# mapeando o caminho do conteiner
CAMINHO_BASE = "/opt/airflow/ws_data_dados/data/"

ARQUIVOS_PARA_DOWNLOAD = {
    "raw_google": f"{CAMINHO_BASE}/google.csv",
    "raw_ifood": f"{CAMINHO_BASE}/ifood.csv",
    "raw_rfb": f"{CAMINHO_BASE}/rfb.csv"
}

@dag(
    dag_id="ingestion_raw_data",
    start_date=datetime(2026,4,14),
    schedule=None,
    catchup=False,
    tags=["raw", "python", "ingestion"],
)
def ingestion_raw_data():
    @task
    def ler_csv_e_carrega(nome_tabela: str, caminho_arquivo: str):
        """Ele vai iterar cada tabela e fazer ser visível no docker e salva na raw do Postgres"""

        if not os.path.exists(caminho_arquivo):
            raise FileNotFoundError(
                f"Arquivo não encontrado: {caminho_arquivo}"
                "Verifique o caminho do docker se está mapeado"
            )
        
        hook = PostgresHook(postgres_conn_id="postgres_ws_data")
        engine = hook.get_sqlalchemy_engine()

        # Le o arquivo local usando pandas

        logging.info(f"Lendo o arquivo: {caminho_arquivo}")
        df = pd.read_csv(caminho_arquivo)

        logging.info(f"Realizando DROP CASCADE na tabela raw.{nome_tabela} para limpar as dependências...")
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS raw.{nome_tabela} CASCADE;"))

        # Salva a tabela no banco na raw

        df.to_sql(
            name=nome_tabela,
            con=engine,
            schema='raw',
            if_exists='replace',
            index=False
        )

        logging.info(f"Sucesso {len(df)} linhas inseridas na tabela raw.{nome_tabela}")

    # Executa a função para dar carga nos arquivos

    for tabela, arquivo in ARQUIVOS_PARA_DOWNLOAD.items():
        ler_csv_e_carrega(nome_tabela=tabela, caminho_arquivo=arquivo)

# Instancia da DAG

dag_instancia = ingestion_raw_data()