import os
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, LoadMode, TestBehavior
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
from pathlib import Path


project = "cdp-customer-data-platform"
dataset = "online_retail"
bucket = "project-dataset-kaggle"
keyfile = "/usr/local/airflow/include/GCP/cdp-customer-data-platform-51fff5265ec4.json"
dbt_project_path = Path("/usr/local/airflow/dags/dbt/online_retail_transform")
dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
dbt_env_path = Path("/usr/local/airflow/dbt_venv/bin/activate")

# Config for Cosmos
profile_config = ProfileConfig(
    profile_name="online_retail_transform",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="gcp",
        profile_args={
            "project":f"{project}",
            "dataset":f"{dataset}",
            "keyfile":f"{keyfile}"
        },
    ),
    # Using profile_mapping or config below
    # profiles_yml_filepath = Path("/usr/local/airflow/dags/dbt/online_retail_transform/profiles.yml")
)
project_config =  ProjectConfig(dbt_project_path = dbt_project_path)
execution_config = ExecutionConfig(dbt_executable_path = dbt_executable_path,)
render_config = RenderConfig(
    load_method=LoadMode.AUTOMATIC,
    select=["path:models/online_retail"],
    test_behavior=TestBehavior.AFTER_EACH
)

def set_path_render_config(path):
    return RenderConfig(
        load_method=LoadMode.AUTOMATIC,
        select=path,
        test_behavior=TestBehavior.AFTER_EACH
    )

data_context_config = None