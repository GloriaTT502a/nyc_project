Overview
========

Welcome to NYC project! 

This project uses 
    1 [Astro](https://www.astronomer.io/dg/signup-airflow/?utm_term=astro%20airflow&utm_campaign=brand-ft-global&utm_source=adwords&utm_medium=ppc&hsa_acc=4274135664&hsa_cam=21865965766&hsa_grp=169329542829&hsa_ad=743940119888&hsa_src=g&hsa_tgt=kwd-1777215821248&hsa_kw=astro%20airflow&hsa_mt=p&hsa_net=adwords&hsa_ver=3&gad_source=1&gad_campaignid=21865965766&gbraid=0AAAAADP7Y9h6CsvefFMH8xTG9Q-_USBQ8&gclid=Cj0KCQjwoZbBBhDCARIsAOqMEZUoRqNGZytzbavrQZdXT9hYyecnTyi5p1hJg3NOWR7pZm2bxNfPb_YaAoMIEALw_wcB) 
    2 [Cosmos](https://www.astronomer.io/cosmos/) 
    3 [DBT](https://www.getdbt.com/product/what-is-dbt) 
    4 [Apache Airflow](https://airflow.apache.org/)
    5 BigQuery 
    6 Google Bucket 
    7 [Soda](https://www.soda.io/) 
    8 Power BI
to analyze the taxi in new york city. 



Project Contents
================


Workflow 
================
![workflow diagram](https://github.com/GloriaTT502a/nyc_project/blob/img/img/workflow.png)



Data Model Structure
===========================


Data Quality Control 
=========================== 

1. table data test step in dbt

2. data cleaning 
    Separate data as valid and invalid data according to data cleaning rules for green and yellow taxi records. 

    Data cleaning rules for green taxt records: 
    
    1) green_warn_rules:
        - name: pickup_datetime_future
          condition: "pickup_datetime <= current_timestamp()"
          description: "Pickup datetime should not be in the future"
        - name: invalid_fare_amount
          condition: "fare_amount >= 0"
          description: "Fare amount should be non-negative"
        - name: missing_passenger_count
          condition: "passenger_count IS NOT NULL"
          description: "Passenger count should not be null"
        - name: unusual_trip_distance
          condition: "trip_distance BETWEEN 0 AND 100"
          description: "Trip distance should be within 0-100 miles"

    2) green_drop_rules:
        - name: null_pickup_datetime
          condition: "pickup_datetime IS NOT NULL"
          description: "Pickup datetime must not be null"
        - name: null_dropoff_datetime
          condition: "dropoff_datetime IS NOT NULL"
          description: "Dropoff datetime must not be null"
        - name: negative_total_amount
          condition: "total_amount > 0"
          description: "Total amount must be positive"                                


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

          