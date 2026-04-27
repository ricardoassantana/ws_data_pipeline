from datetime import datetime
from airflow.sdk import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig
)

# ═══════════════════════════════════════════════════════════════
# CONFIGURAÇÕES DO DBT (COSMOS)
# ═══════════════════════════════════════════════════════════════
DBT_PROJECT_PATH = '/opt/airflow/dbt/ws_data'
DBT_PROFILE_PATH = '/opt/airflow/dbt/ws_data/profiles.yml'
DBT_EXECUTABLE_PATH = '/home/airflow/.local/bin/dbt'

profile_config = ProfileConfig(
    profile_name='ws_data',
    target_name='prd',
    profiles_yml_filepath=DBT_PROFILE_PATH
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

# ═══════════════════════════════════════════════════════════════
# A DAG ORQUESTRADORA (Master DAG Unificada)
# ═══════════════════════════════════════════════════════════════
@dag(
    schedule=None,
    start_date=datetime(2026, 4, 14),
    catchup=False,
    tags=["master", "orquestracao", "dbt", "ws_data"],
    default_args={"retries": 0},
)
def master_ws_data_orchestrator():
    
    # PASSO 1: Dispara a Ingestão do Dado Bruto (Mantemos o Trigger pois funcionou perfeitamente)
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion_raw_data",
        trigger_dag_id="ingestion_raw_data", 
        wait_for_completion=True,            
        poke_interval=30,                    
        deferrable=False,                    
    )

    # PASSO 2: Transformações dbt (OTIMIZADO)
    # Ao invés de 3 grupos pesados, criamos 1 grupo leve.
    # O Cosmos vai ler o dbt apenas UMA VEZ e desenhar a linhagem inteira!
    transformacoes_dbt = DbtTaskGroup(
        group_id="transformacoes_medalhao",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        # Você pode listar as 3 tags juntas, ou simplesmente remover a linha 'render_config'
        # para ele ler a pasta models inteira automaticamente.
        render_config=RenderConfig(select=["bronze", "silver", "gold"]) 
    )

    # ═══════════════════════════════════════════════════════════════
    # ORDEM DE EXECUÇÃO
    # ═══════════════════════════════════════════════════════════════
    trigger_ingestion >> transformacoes_dbt

# Instanciando a DAG para o Airflow reconhecer
master_ws_data_orchestrator()