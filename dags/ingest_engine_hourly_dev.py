from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime



with DAG(
    dag_id="ingest_engine_hourly_dev",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
) as dag:

    iss_now = BashOperator(
        task_id="iss_now",
        bash_command=(
            f"cd /opt/airflow/ingest_engine && python3 main.py "
            f"-c config/import/iss_now.yaml "
            f"-e dev "
            "2>&1"
        )
    )

    iss_now