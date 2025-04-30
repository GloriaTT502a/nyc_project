{{ config(
    materialized='table',
    alias='green_invalid_data'
) }}

SELECT
    pickup_datetime,
    dropoff_datetime,
    fare_amount,
    passenger_count,
    trip_distance,
    total_amount,
    quality_warn,
    quality_drop,
    fixes_applied
FROM {{ ref('green_validations') }}
WHERE ARRAY_LENGTH(quality_drop) > 0  
OR ARRAY_LENGTH(quality_warn) > 0-- Only keep records with non-empty quality_drop