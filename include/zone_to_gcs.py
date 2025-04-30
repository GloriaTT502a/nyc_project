import io
import os
import requests
import pandas as pd
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook


# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "nyc_project_sigma_heuristic")

def upload_to_gcs(bucket, object_name, local_file, gcp_conn_id='gcp'):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    hook.upload(bucket_name=bucket, object_name=object_name, filename=local_file)

def zone_to_gcs(gcp_conn_id='gcp'):
    file_name = "taxi_zone_lookup.csv"
    request_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv" 
    
    r = requests.get(request_url, stream=True)
    

    df = pd.read_csv(io.BytesIO(r.content)) 
    file_name = file_name.replace('.csv', '.parquet')
    df.to_parquet(file_name, engine='pyarrow') 

    # upload it to gcs 
    upload_to_gcs(BUCKET, f"zone/{file_name}", file_name, gcp_conn_id)
    print(f"GCS: zone/{file_name}")