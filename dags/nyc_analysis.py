from airflow.decorators import dag, task 
from datetime import datetime 
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from astro import sql as aql 
from astro.files import File 
from airflow.models.baseoperator import chain
from astro.sql.table import Table, Metadata 
from astro.constants import FileType
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

from include.web_to_gcs import web_to_gcs
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator  # Correct import 
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator 

import os

BUCKET = os.environ.get("GCP_GCS_BUCKET", "nyc_project_sigma_heuristic")

@dag(
    dag_id="nyc_analysis", 
    start_date=datetime(2025, 4, 23), 
    schedule=None, 
    catchup=False, 
    tags=['nyc_analysis'], 
) 

def nyc_analysis(): 
    create_bucket = GCSCreateBucketOperator(
        task_id='create_bucket',
        bucket_name=BUCKET,  
        project_id='sigma-heuristic-457716-n7',
        gcp_conn_id='gcp',
        location='US',  # Match VM zone
        storage_class='STANDARD',
    ) 

    download_green_taxi_data = PythonOperator(
        task_id='download_green_taxi_data',
        python_callable=web_to_gcs,
        op_kwargs={'year': '2019', 'service': 'green', 'gcp_conn_id': 'gcp'},
    )

    create_nyc_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_nyc_dataset',
        dataset_id='nyc_project',
        gcp_conn_id='gcp',
    )

    create_tables = BigQueryExecuteQueryOperator(
        task_id='create_tables',
        gcp_conn_id='gcp',
        sql='create_tables.sql',
        use_legacy_sql=False,  # Use standard SQL for BigQuery
    )

        # Task to load CSV from GCS to BigQuery
    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket=BUCKET,
        source_objects=['green/green_tripdata_2019-*.parquet'],
        destination_project_dataset_table='nyc_project.raw_green_trips',
        source_format='PARQUET',
        gcp_conn_id='gcp',
        write_disposition='WRITE_TRUNCATE', 
        autodetect=False,
        schema_fields=[
            {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "lpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
            {"name": "RatecodeID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "passenger_count", "type": "INTEGER", "mode": "NULLABLE"},  # Fixed to INTEGER
            {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "ehail_fee", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "payment_type", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "trip_type", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "yearmonth", "type": "INTEGER", "mode": "NULLABLE"},
        ], 
        time_partitioning={
            "type": "DAY",
            "field": "lpep_pickup_datetime",
        },
    )

    create_staging_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_staging_dataset',
        dataset_id='staging',
        gcp_conn_id='gcp',
    )

nyc_analysis()