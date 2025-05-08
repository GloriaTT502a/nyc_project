{{ config(
    materialized='table',
    
) }}

WITH datetime_cte AS (
    -- Generate dates from 1990-01-01 to 2030-12-31
    SELECT 
        pickup_datetime AS datestamptime
    FROM 
        {{ ref('stg_nyc__yellow_valid_data') }}
    
    UNION ALL 

    SELECT 
        dropoff_datetime AS datestamptime
    FROM 
        {{ ref('stg_nyc__yellow_valid_data') }} 

    UNION ALL 

    SELECT 
        pickup_datetime AS datestamptime
    FROM 
        {{ ref('stg_nyc__green_valid_data') }}
    
    UNION ALL 

    SELECT 
        dropoff_datetime AS datestamptime
    FROM 
        {{ ref('stg_nyc__green_valid_data') }} 

    UNION ALL 

    SELECT 
        pickup_datetime AS datestamptime
    FROM 
        {{ ref('stg_nyc__fhv_valid_data') }}
    
    UNION ALL 

    SELECT 
        dropoff_datetime AS datestamptime
    FROM 
        {{ ref('stg_nyc__fhv_valid_data') }} 

    UNION ALL 

    SELECT 
        pickup_datetime AS datestamptime
    FROM 
        {{ ref('stg_nyc__fhvhv_valid_data') }}
    
    UNION ALL 

    SELECT 
        dropoff_datetime AS datestamptime
    FROM 
        {{ ref('stg_nyc__fhvhv_valid_data') }} 


), 
deduplicated AS 
(
    SELECT 
        datestamptime 
    FROM datetime_cte 
    GROUP BY 
        datestamptime 
), 
date_dim AS 
(
    SELECT
        UNIX_SECONDS(datestamptime) AS datetime_id, -- String format: "1990-01-01"
        datestamptime AS datestamptime, -- DATE type
        EXTRACT(YEAR FROM datestamptime) AS year,
        EXTRACT(MONTH FROM datestamptime) AS month,
        EXTRACT(DAY FROM datestamptime) AS day,
        EXTRACT(HOUR FROM datestamptime) AS hour, 
        EXTRACT(MINUTE FROM datestamptime) AS minute, 
        EXTRACT(DAYOFWEEK FROM datestamptime) AS weekday -- 1=Sunday, 2=Monday, ..., 7=Saturday
    FROM deduplicated
)
SELECT * FROM date_dim 