{{ config(
    materialized='table',
    
) }}

WITH final AS 
(
    SELECT 
        locationid, 
        borough, 
        zone, 
        service_zone 
    FROM {{ ref('stg_nyc__taxi_zone_lookup') }}
)
SELECT * FROM final 