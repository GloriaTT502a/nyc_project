{{ config(
    materialized='table',
    
) }}

WITH unioned_data AS 
(
    SELECT 
        pickup_locationid, 
        dropoff_locationid 
    FROM 
        {{ ref('stg_nyc__yellow_valid_data') }} 
    
    UNION ALL 
    
    SELECT 
        pickup_locationid, 
        dropoff_locationid  
    FROM 
        {{ ref('stg_nyc__green_valid_data') }} 

    UNION ALL 
    
    SELECT 
        pickup_locationid, 
        dropoff_locationid  
    FROM 
        {{ ref('stg_nyc__fhv_valid_data') }} 

    UNION ALL 
    
    SELECT 
        pickup_locationid, 
        dropoff_locationid  
    FROM 
        {{ ref('stg_nyc__fhvhv_valid_data') }}               
    
), 
deduplicated AS 
(
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['pickup_locationid', 'dropoff_locationid']) }} as location_id, 
        pickup_locationid, 
        dropoff_locationid 
    FROM 
        unioned_data 
    GROUP BY  
        pickup_locationid, 
        dropoff_locationid    
) 
SELECT * FROM deduplicated 