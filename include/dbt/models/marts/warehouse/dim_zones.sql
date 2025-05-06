{{ config(materialized='table') }}

select 
    locationid, 
    borough, 
    zone, 
    service_zone 
from {{ ref('stg_nyc__taxi_zone_lookup') }}