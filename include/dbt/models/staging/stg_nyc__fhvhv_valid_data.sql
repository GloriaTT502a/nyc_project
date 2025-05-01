{{ config(
    materialized='table',
) }}

WITH valid_data AS 
(
    SELECT
        tripid,  
        pickup_locationid, 
        dropoff_locationid, 
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth,  
        dispatching_base_num, 
        hvfhs_license_num, 
        sr_flag 
    FROM {{ ref('stg_nyc__fhvhv_validations') }}
    WHERE ARRAY_LENGTH(quality_drop) = 0 
    AND ARRAY_LENGTH(quality_warn) = 0 -- Only keep records with empty quality_drop
)
SELECT * FROM valid_data 