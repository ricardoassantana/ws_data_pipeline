from datetime import datetime
from airflow.sdk import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ═══════════════════════════════════════════════════════════════
# A DAG ORQUESTRADORA (Master DAG)
# ═══════════════════════════════════════════════════════════════
@dag(
    schedule="0 1 * * *", # A orquestradora assume o horário oficial (01:00)
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["master", "orquestracao", "ws_data"],
    default_args={"retries": 1},
)
def master_ws_data_orchestrator():
    
    #  Dispara a Ingestão do Dado Bruto
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion_raw_data",
        trigger_dag_id="ingestion_raw_data", # Id da dag
        wait_for_completion=True,            # So roda após o sucesso.
        poke_interval=30,                    # De 30 em 30 segundos checa se a ingestão deu certo.
    )

    # : Dispara a Transformação (dbt)
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transformation",
        trigger_dag_id="dbt_ws_data_pipeline", 
        wait_for_completion=True,
    )


    trigger_ingestion >> trigger_dbt

# Instanciando a DAG para o Airflow reconhecer
master_ws_data_orchestrator()