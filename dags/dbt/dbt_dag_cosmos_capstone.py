import os
from datetime import datetime, timedelta
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from airflow.decorators import dag
from dotenv import load_dotenv

#  Dynamically fetch AIRFLOW_HOME and load .env
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
dbt_env_path = os.path.join(AIRFLOW_HOME, 'dbt_project', '.env')
load_dotenv(dbt_env_path)

#  Use AIRFLOW_HOME to construct project and profiles paths
PATH_TO_DBT_PROJECT = os.path.join(AIRFLOW_HOME, 'dbt_project')
PATH_TO_DBT_PROFILES = os.path.join(PATH_TO_DBT_PROJECT, 'profiles.yml')

#  Cosmos profile config
profile_config = ProfileConfig(
    profile_name="capstone_profile",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)

#  Default arguments
default_args = {
    "owner": "stephengeorge93",
    "retries": 2,
    "execution_timeout": timedelta(hours=1),
}

# DAG definition
@dag(
    dag_id="capstone_dbt_pipeline_split_v10",
    start_date=datetime(2025, 6, 1),
    schedule='@once',  # or '@once' for testing
    catchup=False,
    is_paused_upon_creation=False,  #  Ensures DAG is active on first deploy
    default_args=default_args,
    concurrency=1,
)
def capstone_dbt_pipeline():
    #  Run staging models
    dbt_run_staging = DbtTaskGroup(
        group_id="run_staging_models",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=os.path.join(AIRFLOW_HOME, "dbt_venv", "bin", "dbt")  #  Dynamic executable path
        ),
        render_config=RenderConfig(select=["staging"]),
    )

    # Run marts models
    dbt_run_marts = DbtTaskGroup(
        group_id="run_marts_models",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=os.path.join(AIRFLOW_HOME, "dbt_venv", "bin", "dbt")  # Dynamic executable path
        ),
        render_config=RenderConfig(select=["marts"])
    )

    dbt_run_staging >> dbt_run_marts

capstone_dbt_pipeline()
