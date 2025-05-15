{{ config(
    materialized='incremental',
    full_refresh=true,  
    unique_key=['yearmonth', 'industry_type'],

) }}

WITH unioned_data AS 
(
    SELECT 
        yearmonth, 
        industry_type_id, 
        pickup_locationid, 
        dropoff_locationid, 
        trip_count 
    FROM 
        {{ ref('fct_nyc__fhv_fhvhv_trip_data') }} 

    UNION ALL 

    SELECT 
        yearmonth, 
        industry_type_id, 
        pickup_locationid, 
        dropoff_locationid, 
        trip_count 
    FROM 
        {{ ref('fct_nyc__green_yellow_trip_data') }}  
), 
nyc_drop AS 
(
    SELECT 
        yearmonth, 
        industry_type_id, 
        zo_drop.borough as drop_borough, 
        zo_drop.zone as drop_zone, 
        zo_drop.service_zone as drop_service_zone, 
        CASE 
        WHEN zo_drop.borough IN ('Queens', 'Bronx', 'Brooklyn', 'Manhattan', 'Staten Island') 
        THEN 'NYC' 
        WHEN zo_drop.borough IS NULL OR zo_drop.borough = 'Unknown' 
        THEN 'Unknown' 
        ELSE 'Non-NYC' END as drop_is_nyc, 
        zo_pick.borough as pick_borough, 
        zo_pick.zone as pick_zone, 
        zo_pick.service_zone as pick_service_zone,
        CASE 
        WHEN zo_pick.borough IN ('Queens', 'Bronx', 'Brooklyn', 'Manhattan', 'Staten Island') 
        THEN 'NYC' 
        WHEN zo_pick.borough IS NULL OR zo_pick.borough = 'Unknown' 
        THEN 'Unknown' 
        ELSE 'Non-NYC' END as pick_is_nyc,  
        SUM(trip_count) as TRIP_CNT 
    FROM 
        unioned_data   
    JOIN 
        {{ ref('dim_nyc__zones') }} zo_drop  
    ON unioned_data.dropoff_locationid = zo_drop.locationid 
    JOIN {{ ref('dim_nyc__zones') }} zo_pick 
    ON unioned_data.pickup_locationid = zo_pick.locationid 
    GROUP BY 
        yearmonth, 
        industry_type_id, 
        zo_drop.borough, 
        zo_drop.zone, 
        zo_drop.service_zone,
        zo_pick.borough, 
        zo_pick.zone, 
        zo_pick.service_zone 
)
SELECT * FROM nyc_drop 



