{{ config(
    materialized='incremental',
    full_refresh=true,  
    unique_key=['yearmonth', 'industry_type'],
    pre_hook=[
    "truncate table {{ this }} "
    ]

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
YTD_Trip AS 
(
    SELECT 
        cast(yearmonth/100 as integer) as year,  
        industry_type_id, 
        pickup_locationid, 
        dropoff_locationid, 
        sum(trip_count) AS TRIP_CNT
    FROM 
        unioned_data  
    WHERE yearmonth >= cast(yearmonth/100 as integer)*100+1 AND 
        yearmonth <= cast(yearmonth/100 as integer)*100+12  
    GROUP BY 
        cast(yearmonth/100 as integer), 
        industry_type_id, 
        pickup_locationid, 
        dropoff_locationid   
 
), 
nyc_drop AS 
(
    SELECT 
        year, 
        industry_type_id, 
        zo_drop.borough as drop_borough, 
        zo_drop.zone as drop_zone, 
        zo_drop.service_zone as drop_service_zone, 
        TRIP_CNT 
    FROM 
        YTD_Trip  
    JOIN 
        {{ ref('dim_nyc__zones') }} zo_drop  
    ON YTD_Trip.dropoff_locationid = zo_drop.locationid 
    JOIN {{ ref('dim_nyc__zones') }} zo_pick 
    ON YTD_Trip.pickup_locationid = zo_pick.locationid  
)
SELECT * FROM nyc_drop 



