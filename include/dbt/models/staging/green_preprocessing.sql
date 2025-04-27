{{ 
    config(
    materialized='table'
    ) 
}}

WITH raw_data AS (
    SELECT
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        fare_amount,
        passenger_count,
        trip_distance,
        total_amount
    FROM {{ source('base','raw_green_trips') }}
    
)
SELECT
    *,
    [] AS fixes_applied  -- initialize the field for data quality mark
FROM raw_data