from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import os
from datetime import datetime

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={"schema": "public"},
    ),
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        "/usr/local/airflow/dags/dbt/online_retail_transform",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),operator_args={
        "py_system_site_packages": False,
        "py_requirements": ["dbt-bigquery"],
    },
    # normal dag parameters
    schedule_interval="@daily",
    start_date= datetime(2023, 1, 1),
    catchup=False,
    dag_id="my_cosmos_dag",
)