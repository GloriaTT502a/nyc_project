NYC Taxi Data Warehouse Project
========

Overview
======== 


This project implements a three-layer data warehouse architecture (staging, warehouse, reporting) to process and analyze New York City (NYC) taxi trip data. It leverages open-source tools and Google Cloud Platform (GCP) services to orchestrate, transform, and visualize taxi trip data, enabling insights into urban transportation patterns. 


Technologies Used
====================

1. [Astro](https://www.astronomer.io/dg/signup-airflow/?utm_term=astro%20airflow&utm_campaign=brand-ft-global&utm_source=adwords&utm_medium=ppc&hsa_acc=4274135664&hsa_cam=21865965766&hsa_grp=169329542829&hsa_ad=743940119888&hsa_src=g&hsa_tgt=kwd-1777215821248&hsa_kw=astro%20airflow&hsa_mt=p&hsa_net=adwords&hsa_ver=3&gad_source=1&gad_campaignid=21865965766&gbraid=0AAAAADP7Y9h6CsvefFMH8xTG9Q-_USBQ8&gclid=Cj0KCQjwoZbBBhDCARIsAOqMEZUoRqNGZytzbavrQZdXT9hYyecnTyi5p1hJg3NOWR7pZm2bxNfPb_YaAoMIEALw_wcB): Orchestrates workflows using Apache Airflow. 
2. [Cosmos](https://www.astronomer.io/cosmos/): Integrates Apache Airflow with dbt for seamless data transformation. 
3. [DBT](https://www.getdbt.com/product/what-is-dbt): Handles data transformation and modeling.
4. [Apache Airflow](https://airflow.apache.org/): Manages workflow orchestration.
5. BigQuery: Serves as the compute engine for data processing.
6. Google Bucket: Stores raw and processed data.
7. Power BI: Validates and visualizes reporting layer data. 

to implements a three-layer data warehouse architecture (staging, warehouse, reporting) to process and analyze taxi trip in New York. 



Features
================
- Three-Layer Data Model: Implements staging, warehouse, and reporting layers for efficient data processing.

- Open-Source Tools: Utilizes Apache Airflow and dbt for robust workflow and transformation pipelines.

- Cloud Integration: Leverages GCP for scalable storage and compute capabilities.

- Data Quality Assurance: Includes validation and cleaning rules to ensure high-quality data.


Prerequisites
================ 
To run this project, ensure the following are installed and configured: 

- Python 3.10 
- Docker: For containerized deployment of Astro and Airflow.
- Astro CLI: For managing Airflow and dbt workflows.
- Google Cloud Platform (GCP): Account with access to BigQuery and Cloud Storage.
- Power BI Desktop: For report visualization and validation.
- GCP Credentials: Service account key with permissions for BigQuery and Cloud Storage.


Architecture 
================
The project follows a modular architecture, as illustrated below:
![workflow diagram](https://github.com/GloriaTT502a/nyc_project/blob/img/img/workflow.png)

- ### Data Ingestion: 
  Raw NYC taxi data (yellow, green, FHV, FHVHV) is ingested from Google Cloud Storage.
- ### Orchestration: 
  Astro and Airflow manage the ETL pipeline.- - 
- ### Transformation: 
  dbt processes data through staging, warehouse, and reporting layers.
- ### Storage and Compute: 
  BigQuery stores and processes transformed data; Cloud Storage holds raw and intermediate files.
- ### Visualization: 
  Power BI connects to BigQuery for report generation and validation. 


Data Model Structure
===========================
![Data Model Structure](https://github.com/GloriaTT502a/nyc_project/blob/img/img/Three_Layers_DataModeling.png)

Three_Layers_DataModeling: 

It typically consists of staging, warehouse, and reporting layers, each serving distinct purposes to ensure data is efficiently processed, stored, and presented for analysis. 

- ### Staging Layer:
  ### Purpose: 
      Validates, cleanses, and deduplicates raw data to ensure quality.
  ### Tasks: 
      Loads raw taxi data, applies initial cleaning rules, and prepares data for transformation.

- ### Warehouse Layer:
  ### Purpose: 
      Integrates and transforms data into a structured format (dimension and fact tables).
  ### Tasks: 
      Uses dbt to create dimension (e.g., time, location) and fact (e.g., trip details) tables.

- ### Reporting Layer:
  ### Purpose: 
      Aggregates data and generates KPIs for business intelligence tools.
  ### Tasks: 
      Produces summarized tables and metrics for Power BI integration.


Data Quality Control 
=========================== 

### Test tables in dbt 
  - Test for staging tables 
    
    1) For valid tables, data is not null. 
      ![Test for staging tables](https://github.com/GloriaTT502a/nyc_project/blob/img/img/Test_staging_table.png)

  - Test for warehouse tables 
      
    1) For dim tables, primary key is unique and not null. 
      ![Test for warehouse tables](https://github.com/GloriaTT502a/nyc_project/blob/img/img/Test_dim_table.png)

      
    2) For fact tables, the foreign key can be found in primary key. 
      ![Test for fact tables](https://github.com/GloriaTT502a/nyc_project/blob/img/img/Test_fact_table.png)

### Data cleaning 
    
Separate data as valid and invalid data according to data cleaning rules for green, yellow, fhv and fhvhv taxi records. 

![Datavalidation1](https://github.com/GloriaTT502a/nyc_project/blob/img/img/Datavalidationgreen.png)

![Datavalidation2](https://github.com/GloriaTT502a/nyc_project/blob/img/img/Datavalidationfhv.png)

- For green and yellow taxi records: 
  
  The green and yellow taxi records with below conditions will be deleted from valid table: 

    {'name': 'pickup_datetime_null', 'condition': 'pickup_datetime IS NULL'},
    {'name': 'dropoff_datetime_null', 'condition': 'dropoff_datetime IS NULL'},
    {'name': 'fare_amount_negative', 'condition': 'fare_amount < 0'},
    {'name': 'passenger_count_zero', 'condition': 'passenger_count = 0'},
    {'name': 'trip_distance_negative', 'condition': 'trip_distance < 0'},
    {'name': 'total_amount_negative', 'condition': 'total_amount < 0'}
    {'name': 'pickup_datetime_invalid', 'condition': 'pickup_datetime > CURRENT_TIMESTAMP()'},
    {'name': 'dropoff_datetime_invalid', 'condition': 'dropoff_datetime > CURRENT_TIMESTAMP()'},
    {'name': 'pickup_dropoff_invalid', 'condition': 'pickup_datetime > dropoff_datetime'},
    {'name': 'fare_amount_null', 'condition': 'fare_amount IS NULL'},
    {'name': 'total_amount_null', 'condition': 'total_amount IS NULL'}    
    
- For fhv and fhvhv taxi records: 
  
  The fhv and fhvhv taxi records with below conditions will be deleted from valid table:  

    {'name': 'pickup_datetime_null', 'condition': 'pickup_datetime IS NULL'},
    {'name': 'dropoff_datetime_null', 'condition': 'dropoff_datetime IS NULL'},
    {'name': 'pickup_locationid_negative', 'condition': 'pickup_locationid < 0'},
    {'name': 'dropoff_locationid_negative', 'condition': 'dropoff_locationid < 0'} 
    {'name': 'pickup_datetime_invalid', 'condition': 'pickup_datetime > CURRENT_TIMESTAMP()'},
    {'name': 'dropoff_datetime_invalid', 'condition': 'dropoff_datetime > CURRENT_TIMESTAMP()'},
    {'name': 'pickup_dropoff_invalid', 'condition': 'pickup_datetime > dropoff_datetime'} 







    Data cleaning rules for yellow taxt records: 

    1) yellow_warn_rules:
        - name: pickup_datetime_future:
          condition: "pickup_datetime <= current_timestamp()"
          description: "Pickup datetime should not be in the future"
        - name: invalid_fare_amount:
          condition: "fare_amount >= 0"
          description: "Fare amount should be non-negative"
        - name: missing_passenger_count:
          condition: "passenger_count IS NOT NULL"
          description: "Passenger count should not be null"
        - name: unusual_trip_distance:
          condition: "trip_distance BETWEEN 0 AND 100"
          description: "Trip distance should be within 0-100 miles"

    2) yellow_drop_rules:
        - name: null_pickup_datetime:
          condition: "pickup_datetime IS NOT NULL"
          description: "Pickup datetime must not be null"
        - name: null_dropoff_datetime:
          condition: "dropoff_datetime IS NOT NULL"
          description: "Dropoff datetime must not be null"
        - name: invalid_location:
          condition: "pickup_longitude BETWEEN -74.3 AND -73.7 AND pickup_latitude BETWEEN 40.5 AND 41.0"
          description: "Pickup location must be within NYC bounds"
        - name: negative_total_amount:
          condition: "total_amount > 0"
          description: "Total amount must be positive"

          