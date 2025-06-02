import os
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# Dynamically set path to the script using AIRFLOW_HOME
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
SCRIPT_PATH = os.path.join(AIRFLOW_HOME, 'include', 'eczachly', 'scripts', 'write_to_iceberg.py')

default_args = {
    "owner": "stephengeorge93",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="api_ingestion_dag_v3",
    start_date=datetime(2025, 6, 1),
    schedule='@once',  # or @once for testing
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
    tags=["api", "rentcast"]
)
def api_ingestion_dag():
    extract_data = BashOperator(
        task_id="extract_rentcast_data",
        bash_command=f"python {SCRIPT_PATH}"
    )

    extract_data

api_ingestion_dag()