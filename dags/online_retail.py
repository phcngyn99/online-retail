from airflow.decorators import dag, task
from pendulum import datetime, duration
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

from cosmos import DbtTaskGroup
from airflow.models.baseoperator import chain
import config


@dag(
    start_date= datetime(2023,1,1),
    schedule= None,
    catchup= False,
    tags= ['online_retail'],
    # default_args={
    #     "retries": 3,
    #     "retry_delay": duration(seconds=2),
    #     "retry_exponential_backoff": True,
    #     "max_retry_delay": duration(minutes=2),
    # },
)

def online_retail():
    
    upload_csv_to_gcs= LocalFilesystemToGCSOperator(
        task_id= "upload_csv_to_gcs",
        src= "/usr/local/airflow/include/dataset/online_retail.csv",
        dst= "raw/online_retail.csv",
        bucket= f"{config.bucket}",
        gcp_conn_id= "gcp",
        mime_type= "text/csv"
    )
    
    create_bq_dataset= BigQueryCreateEmptyDatasetOperator(
        task_id = "create_bq_dataset",
        dataset_id= f"{config.dataset}",
        gcp_conn_id= "gcp",
        if_exists= "ignore"
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id = "gcs_to_bq",
        bucket= f"{config.bucket}",
        source_objects="raw/online_retail.csv",
        destination_project_dataset_table= f"{config.project}.{config.dataset}.raw",
        gcp_conn_id= "gcp",
        schema_fields= [
            {"name": "InvoiceNo", "type": "STRING", "mode": "NULLABLE"},
            {"name": "StockCode", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Quantity", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "InvoiceDate", "type": "STRING", "mode": "NULLABLE"},
            {"name": "UnitPrice", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "CustomerID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Country", "type": "STRING", "mode": "NULLABLE"}
        ],
        write_disposition= "WRITE_TRUNCATE" #Overwrite if existed
    )
    
    transform = DbtTaskGroup(
        group_id= "transform",
        project_config = config.project_config,
        profile_config = config.profile_config,
        execution_config= config.execution_config,
        render_config = config.render_config
    )

    #quality check
    gx_validate_bq = GreatExpectationsOperator(
        task_id = "quality_check",
        conn_id= "gcp",
        data_context_root_dir= "include/gx",
        expectation_suite_name= "online_retail_suite",
        data_asset_name= f"{config.project}.{config.dataset}.raw",
        return_json_dict= True,
    )

    chain(
        upload_csv_to_gcs,
        create_bq_dataset,
        gcs_to_bq,
        transform
    )
    
online_retail()