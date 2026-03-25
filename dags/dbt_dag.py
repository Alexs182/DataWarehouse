import os
import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / ".." 
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROJECT_NAME = "data_warehouse"

dbt_dag = DbtDag(
    dag_id="dbt_pipeline",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,

    project_config=ProjectConfig(
        dbt_project_path="/opt/airflow/data_warehouse",
    ),

    profile_config=ProfileConfig(
        profile_name="data_warehouse",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="pg_dev_warehouse",   # Airflow connection ID
            profile_args={"schema": "staging"},
        ),
    ),

    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.VIRTUALENV,
        dbt_executable_path=Path("/home/airflow/.local/bin/dbt"),  # your dbt venv
    ),

    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,   # fastest & most accurate
    ),

    default_args={"retries": 2, "retry_delay": datetime.timedelta(minutes=5)},
)