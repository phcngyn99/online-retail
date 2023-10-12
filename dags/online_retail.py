import os
from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.models.baseoperator import chain

project = "cdp-customer-data-platform"
dataset = "online_retail"
bucket = "project-dataset-kaggle"

@dag(
    start_date= datetime(2023,1,1),
    schedule= None,
    catchup= False,
    tags= ['online_retail'] #DAG id
)

def online_retail():
    
    upload_csv_to_gcs= LocalFilesystemToGCSOperator(
        task_id= "upload_csv_to_gcs",
        src= "/usr/local/airflow/include/dataset/online_retail.csv",
        dst= "raw/online_retail.csv",
        bucket= f"{bucket}",
        gcp_conn_id= "gcp",
        mime_type= "text/csv"
    )
    
    create_bq_dataset= BigQueryCreateEmptyDatasetOperator(
        task_id = "create_bq_dataset",
        dataset_id= f"{dataset}",
        gcp_conn_id= "gcp",
        if_exists= "ignore"
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id = "gcs_to_bq",
        bucket= f"{bucket}",
        source_objects="raw/online_retail.csv",
        destination_project_dataset_table= f"{project}.{dataset}.raw",
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
    upload_csv_to_gcs >> create_bq_dataset >>gcs_to_bq
online_retail()
    
