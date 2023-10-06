import os
from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

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
        bucket= "project-dataset-kaggle",
        gcp_conn_id= "gcp",
        mime_type= "text/csv"
    )
    
    create_bq_dataset= BigQueryCreateEmptyDatasetOperator(
        task_id = "create_bq_dataset",
        dataset_id= "online_retail",
        gcp_conn_id= "gcp"
    )
    
online_retail()
    
