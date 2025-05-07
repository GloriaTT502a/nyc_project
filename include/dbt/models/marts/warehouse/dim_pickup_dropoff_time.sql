{{ config(
    materialized='table',
    
) }}

WITH unioned_data AS 
(
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['pickup_datetime', 'dropoff_datetime']) }} as pickup_dropoff_time_id, 
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth 
    FROM 
        {{ ref('stg_nyc__yellow_valid_data') }} 
    
    UNION ALL 
    
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['pickup_datetime', 'dropoff_datetime']) }} as pickup_dropoff_time_id, 
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth 
    FROM 
        {{ ref('stg_nyc__green_valid_data') }} 
    
), 
deduplicated AS 
(
    SELECT 
        pickup_dropoff_time_id, 
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth 
    FROM 
        unioned_data 
    GROUP BY 
        pickup_dropoff_time_id, 
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth    
) 
SELECT * FROM deduplicated 