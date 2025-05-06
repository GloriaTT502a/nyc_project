{{ config(
    materialized='table',
) }} 

WITH defined AS 
(
    SELECT  
        locationid, 
        borough, 
        zone, 
        replace(service_zone,'Boro','Green') as service_zone  
    FROM {{ source('base','taxi_zone_lookup') }}
) 
SELECT * FROM defined  