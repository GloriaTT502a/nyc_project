{{ config(
    materialized='table',
    
) }}

-- Create the fact table by joining the relevant keys from dimension table
WITH fct_ff_trip_cte AS (
    SELECT
        tripid AS tripid,
        pickup_locationid, 
        dropoff_locationid,
        UNIX_SECONDS(pickup_datetime) AS pickup_datetime_id,
        UNIX_SECONDS(dropoff_datetime) AS dropoff_datetime_id,
        yearmonth, 
        {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'affiliated_base_number', 'sr_flag']) }} as base_num_sr_id,
        2 AS industry_type_id, 
        1 AS trip_count 
    FROM {{ ref('stg_nyc__fhv_valid_data') }}

    UNION ALL 

    SELECT
        tripid AS tripid,
        pickup_locationid, 
        dropoff_locationid,
        UNIX_SECONDS(pickup_datetime) AS pickup_datetime_id,
        UNIX_SECONDS(dropoff_datetime) AS dropoff_datetime_id,
        yearmonth, 
        {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'hvfhs_license_num', 'sr_flag']) }} as base_num_sr_id,
        3 AS industry_type_id, 
        1 AS trip_count  
    FROM {{ ref('stg_nyc__fhvhv_valid_data') }}

), 
fct_ff_join AS 
(
    SELECT
        tripid,
        pickup_locationid, 
        dropoff_locationid, 
        fct_ff.pickup_datetime_id, 
        fct_ff.dropoff_datetime_id, 
        yearmonth, 
        fct_ff.base_num_sr_id, 
        industry_type_id, 
        trip_count 
    FROM fct_ff_trip_cte fct_ff
)
SELECT * FROM fct_ff_join 