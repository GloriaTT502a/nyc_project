{{ config(
    materialized='table',
) }}

WITH valid_data AS 
(
    SELECT
        tripid, 
        vendorid, 
        ratecodeid, 
        pickup_locationid, 
        dropoff_locationid, 
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth, 
        store_and_fwd_flag, 
        passenger_count, 
        trip_distance, 
        trip_type, 
        trip_type_description, 
        fare_amount, 
        extra, 
        mta_tax, 
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        payment_type_description,
        final_rate_code_description,
        LPEP_provider 
    FROM {{ ref('stg_nyc__green_validations') }}
    WHERE ARRAY_LENGTH(quality_drop) = 0 
    AND ARRAY_LENGTH(quality_warn) = 0 -- Only keep records with empty quality_drop
)
SELECT * FROM valid_data 