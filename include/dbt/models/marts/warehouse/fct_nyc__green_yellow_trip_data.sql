{{ config(
    materialized='table',
    
) }}

-- Create the fact table by joining the relevant keys from dimension table
WITH fct_yellow_green_trip_cte AS (
    SELECT
        tripid AS tripid,
        pickup_locationid, 
        dropoff_locationid, 
        UNIX_SECONDS(pickup_datetime) AS pickup_datetime_id,
        UNIX_SECONDS(dropoff_datetime) AS dropoff_datetime_id,
        yearmonth, 
        {{ dbt_utils.generate_surrogate_key(['payment_type', 'vendorid', 'store_and_fwd_flag', 'ratecodeid']) }} as trip_status_id,
        0 AS industry_type_id, 
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        NULL AS ehail_fee, 
        improvement_surcharge,
        total_amount,
        congestion_surcharge
    FROM {{ ref('stg_nyc__yellow_valid_data') }}

    UNION ALL 

    SELECT
        tripid AS tripid,
        pickup_locationid, 
        dropoff_locationid, 
        UNIX_SECONDS(pickup_datetime) AS pickup_datetime_id,
        UNIX_SECONDS(dropoff_datetime) AS dropoff_datetime_id,
        yearmonth, 
        {{ dbt_utils.generate_surrogate_key(['payment_type', 'vendorid', 'store_and_fwd_flag', 'ratecodeid']) }} as trip_status_id,
        1 AS industry_type_id,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee, 
        improvement_surcharge,
        total_amount,
        congestion_surcharge  
    FROM {{ ref('stg_nyc__green_valid_data') }}

), 
fct_yg_join AS 
(
    SELECT
        tripid,
        pickup_locationid, 
        dropoff_locationid,
        fct_yellow_green.pickup_datetime_id, 
        fct_yellow_green.dropoff_datetime_id, 
        yearmonth, 
        fct_yellow_green.trip_status_id, 
        industry_type_id, 
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee, 
        improvement_surcharge,
        total_amount,
        congestion_surcharge 
    FROM fct_yellow_green_trip_cte fct_yellow_green
)
SELECT * FROM fct_yg_join 