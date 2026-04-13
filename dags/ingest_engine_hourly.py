from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import Variable
from datetime import datetime

ENV = Variable.get("environment")

with DAG(
    dag_id="ingest_engine_hourly",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
) as dag:

    iss_now = BashOperator(
        task_id="iss_now",
        bash_command=(
            f"cd /opt/airflow/ingest_engine && python3 main.py "
            f"-c config/import/api/iss_now.yaml "
            f"-e {ENV} "
            "2>&1"
        )
    )

    coingecko_simpleprice = BashOperator(
        task_id="coingecko_simpleprice",
        bash_command=(
            f"cd /opt/airflow/ingest_engine && python3 main.py "
            f"-c config/import/api/coingecko_simpleprice.yaml "
            f"-e {ENV} "
            "2>&1"
        )
    )

    iss_now >> coingecko_simpleprice