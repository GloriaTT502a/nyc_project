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
from include.zone_to_gcs import zone_to_gcs
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
        task_id=f'download_green_taxi_data',
        python_callable=web_to_gcs,
        op_kwargs={'year': ['2020', '2021'], 'service': 'green', 'gcp_conn_id': 'gcp'},
    )

    download_yellow_taxi_data = PythonOperator(
        task_id='download_yellow_taxi_data',
        python_callable=web_to_gcs,
        op_kwargs={'year': ['2020', '2021'], 'service': 'yellow', 'gcp_conn_id': 'gcp'},
    )    

    download_fhv_taxi_data = PythonOperator(
        task_id='download_fhv_taxi_data',
        python_callable=web_to_gcs,
        op_kwargs={'year': ['2020', '2021'], 'service': 'fhv', 'gcp_conn_id': 'gcp'},
    ) 

    download_fhvhv_taxi_data = PythonOperator(
        task_id='download_fhvhv_taxi_data',
        python_callable=web_to_gcs,
        op_kwargs={'year': '2021', 'service': 'fhvhv', 'gcp_conn_id': 'gcp'},
    ) 

    download_zone_data = PythonOperator(
        task_id='download_zone_data',
        python_callable=zone_to_gcs,
        op_kwargs={'gcp_conn_id': 'gcp'},
    )

    create_base_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_base_dataset',
        dataset_id='base',
        gcp_conn_id='gcp',
    )

    create_tables = BigQueryExecuteQueryOperator(
        task_id='create_tables',
        gcp_conn_id='gcp',
        sql='create_tables.sql',
        use_legacy_sql=False,  # Use standard SQL for BigQuery
    )

    # Task to load CSV from GCS to BigQuery
    load_green_to_bigquery = GCSToBigQueryOperator(
        task_id='load_green_to_bigquery',
        bucket=BUCKET,
        source_objects=['green/green_tripdata_*.parquet'],
        destination_project_dataset_table='base.raw_green_trips',
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

    load_yellow_to_bigquery = GCSToBigQueryOperator(
        task_id='load_yellow_to_bigquery',
        bucket=BUCKET,
        source_objects=['yellow/yellow_tripdata_*.parquet'],
        destination_project_dataset_table='base.raw_yellow_trips',
        source_format='PARQUET',
        gcp_conn_id='gcp',
        write_disposition='WRITE_TRUNCATE', 
        autodetect=False,
        schema_fields=[
            {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "tpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "tpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "passenger_count", "type": "INTEGER", "mode": "NULLABLE"},  # Fixed to INTEGER
            {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "RatecodeID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "payment_type", "type": "INTEGER", "mode": "NULLABLE"},           
            {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "yearmonth", "type": "INTEGER", "mode": "NULLABLE"},
        ], 
        time_partitioning={
            "type": "DAY",
            "field": "tpep_pickup_datetime",
        },
    )    

    load_fhv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_fhv_to_bigquery',
        bucket=BUCKET,
        source_objects=['fhv/fhv_tripdata_*.parquet'],
        destination_project_dataset_table='base.raw_fhv_trips',
        source_format='PARQUET',
        gcp_conn_id='gcp',
        write_disposition='WRITE_TRUNCATE', 
        autodetect=False,
        schema_fields=[
            {"name": "dispatching_base_num", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "dropOff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "PUlocationID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "DOlocationID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "SR_Flag", "type": "INTEGER", "mode": "NULLABLE"},           
            {"name": "Affiliated_base_number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "yearmonth", "type": "INTEGER", "mode": "NULLABLE"},
        ], 
        time_partitioning={
            "type": "DAY",
            "field": "pickup_datetime",
        },
    )

    load_fhvhv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_fhvhv_to_bigquery',
        bucket=BUCKET,
        source_objects=['fhvhv/fhvhv_tripdata_2021-*.parquet'],
        destination_project_dataset_table='base.raw_fhvhv_trips',
        source_format='PARQUET',
        gcp_conn_id='gcp',
        write_disposition='WRITE_TRUNCATE', 
        autodetect=False,
        schema_fields=[
            {"name": "hvfhs_license_num", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dispatching_base_num", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "SR_Flag", "type": "INTEGER", "mode": "NULLABLE"},           
            {"name": "yearmonth", "type": "INTEGER", "mode": "NULLABLE"}, 
        ], 
        time_partitioning={
            "type": "DAY",
            "field": "pickup_datetime",
        },
    )    

    load_zone_to_bigquery = GCSToBigQueryOperator(
        task_id='load_zone_to_bigquery',
        bucket=BUCKET,
        source_objects=['zone/taxi_zone_lookup.parquet'],
        destination_project_dataset_table='base.taxi_zone_lookup',
        source_format='PARQUET',
        gcp_conn_id='gcp',
        write_disposition='WRITE_TRUNCATE', 
        autodetect=False,
        schema_fields=[
            {"name": "locationid", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "borough", "type": "STRING", "mode": "NULLABLE"},
            {"name": "zone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "service_zone", "type": "STRING", "mode": "NULLABLE"}, 
        ], 
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    check_load()

    create_staging_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_staging_dataset',
        dataset_id='staging',
        gcp_conn_id='gcp',
    )

    create_staging_tables = DbtTaskGroup(
        group_id='create_staging_tables',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/staging'],

        )
    )





nyc_analysis()