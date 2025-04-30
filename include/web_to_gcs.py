import io
import os
import requests
import pandas as pd
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
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


def web_to_gcs(year, service, gcp_conn_id='gcp'):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]
        yearmonth=int(year)*100+i+1 

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # fhvhv only has data in January and June 
        if service == 'fhvhv' and month != '01':
            print(f"Skipping {file_name} for fhvhv (only 01 and 06 available)")
            continue 
            
        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(file_name, compression='gzip') 

        if service == 'green': 
            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'], errors='coerce', format='%Y-%m-%d %H:%M:%S').astype('datetime64[us]')
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'], errors='coerce', format='%Y-%m-%d %H:%M:%S').astype('datetime64[us]')
            df['trip_type'] = df['trip_type'].astype('Int64') 

        if service == 'yellow': 
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce', format='%Y-%m-%d %H:%M:%S').astype('datetime64[us]')
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce', format='%Y-%m-%d %H:%M:%S').astype('datetime64[us]')
            

        if service == 'green' or service == 'yellow': 

            df['VendorID'] = df['VendorID'].astype('Int64') 
            df['RatecodeID'] = df['RatecodeID'].astype('Int64') 
            df['PULocationID'] = df['PULocationID'].astype('Int64') 
            df['DOLocationID'] = df['DOLocationID'].astype('Int64')
            
            df['payment_type'] = df['payment_type'].astype('Int64')
            df['passenger_count'] = df['passenger_count'].astype('Int64') 
            df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype(str)
        
        if service == 'fhv': 
            df['dispatching_base_num'] = df['dispatching_base_num'].astype(str) 
            df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce', format='%Y-%m-%d %H:%M:%S').astype('datetime64[us]')
            df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'], errors='coerce', format='%Y-%m-%d %H:%M:%S').astype('datetime64[us]')
            df['PUlocationID'] = df['PUlocationID'].astype('Int64') 
            df['DOlocationID'] = df['DOlocationID'].astype('Int64')
            df['Affiliated_base_number'] = df['Affiliated_base_number'].astype(str) 
            df['SR_Flag'] = df['SR_Flag'].astype('Int64') 
            
        
        if service == 'fhvhv': 
            df['hvfhs_license_num'] = df['hvfhs_license_num'].astype(str) 
            df['dispatching_base_num'] = df['dispatching_base_num'].astype(str) 
            df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce', format='%Y-%m-%d %H:%M:%S').astype('datetime64[us]')
            df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'], errors='coerce', format='%Y-%m-%d %H:%M:%S').astype('datetime64[us]')
            df['PULocationID'] = df['PULocationID'].astype('Int64') 
            df['DOLocationID'] = df['DOLocationID'].astype('Int64')
            df['SR_Flag'] = df['SR_Flag'].astype('Int64')        
        

        df['yearmonth'] = yearmonth
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name, gcp_conn_id)
        print(f"GCS: {service}/{file_name}")


# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')