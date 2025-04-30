{{ config(
    materialized='table',
    alias='green_valid_data'
) }}

SELECT
    pickup_datetime,
    dropoff_datetime,
    fare_amount,
    passenger_count,
    trip_distance,
    total_amount,
    quality_warn,
    fixes_applied
FROM {{ ref('green_validations') }}
WHERE ARRAY_LENGTH(quality_drop) = 0 
AND ARRAY_LENGTH(quality_warn) = 0 -- Only keep records with empty quality_drop