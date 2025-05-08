{{ config(
    materialized='table',
    
) }}

WITH unioned_data AS 
(
    SELECT 
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth 
    FROM 
        {{ ref('stg_nyc__yellow_valid_data') }} 
    
    UNION ALL 
    
    SELECT 
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth 
    FROM 
        {{ ref('stg_nyc__green_valid_data') }} 

    UNION ALL 
    
    SELECT 
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth 
    FROM 
        {{ ref('stg_nyc__fhv_valid_data') }} 

    UNION ALL 
    
    SELECT 
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth 
    FROM 
        {{ ref('stg_nyc__fhvhv_valid_data') }}               
    
), 
deduplicated AS 
(
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['pickup_datetime', 'dropoff_datetime', 'yearmonth']) }} as pickup_dropoff_time_id, 
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth 
    FROM 
        unioned_data 
    GROUP BY   
        pickup_datetime, 
        dropoff_datetime, 
        yearmonth    
) 
SELECT * FROM deduplicated 