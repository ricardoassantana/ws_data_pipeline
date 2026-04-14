from datetime import datetime
from airflow.sdk import dag, task
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig
)

######## Arquivos dentro do Docker
DBT_PROJECT_PATH = '/opt/airflow/dbt/ws_data'
DBT_PROFILE_PATH = '/opt/airflow/dbt/ws_data/profiles.yml'
DBT_EXECUTABLE_PATH = '/home/airflow/.local/bin/dbt'


profile_config = ProfileConfig(
    profile_name = 'ws_data',
    target_name='prd',
    profiles_yml_filepath=DBT_PROFILE_PATH
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

@dag(
    schedule=None,#"0 1 * * *", # Roda todos os dias à 01:00 da manhã
    start_date=datetime(2026, 4, 14),
    catchup=False,
    tags=["dbt", "medalhao", "ws_data"],
    default_args={"retries": 0},
)
def dbt_ws_data_pipeline():
    ## Executando a camada bronze

    camada_bronze = DbtTaskGroup(
        group_id="camada_bronze",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["bronze"])
    )
# Executor da camada silver
    camada_silver = DbtTaskGroup(
        group_id="camada_silver",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["silver"])
    )

    camada_gold = DbtTaskGroup(
        group_id="camada_gold",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["gold"]),
    )

    camada_bronze >> camada_silver >> camada_gold

# instancia da DAG

dbt_ws_data_pipeline()
