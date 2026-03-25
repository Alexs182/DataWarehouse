from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime


DBT_PROJECT_DIR = "/opt/airflow/data_warehouse"


def get_dbt_env():
    conn = BaseHook.get_connection("dbt_postgres")
    return {
        "DBT_HOST":     conn.host,
        "DBT_USER":     conn.login,
        "DBT_PASSWORD": conn.password,
        "DBT_DBNAME":   conn.schema,
        "DBT_PORT":     str(conn.port),
    }

#default_args = {
#'start_date': datetime(2022, 1, 1),
#}

#dag = DAG(
#    'sample_dag',
#    default_args=default_args,
#    schedule='0 0 * * *', # Run daily at midnight
#)

with DAG(
    dag_id="dbt_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"/home/airflow/.local/bin/dbt run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
        env=get_dbt_env(),   # <-- credentials injected here
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
        env=get_dbt_env(),
    )

    dbt_run >> dbt_test
