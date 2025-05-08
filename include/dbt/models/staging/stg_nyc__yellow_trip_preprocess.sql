{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
    row_number() over(partition by tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid) as rn
  from {{ source('base','raw_yellow_trips') }}
  --where vendorid is not null 
), 
renamed as (
  select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pulocationid', 'dolocationid', "'yellow'"]) }} as tripid, 
    {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} as vendorid, 
    {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,  
    
    -- timestamps 
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime, 
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime, 

    -- yearmonth 
    yearmonth as yearmonth, 

    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount, 
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(congestion_surcharge as numeric) as congestion_surcharge,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description, 
    {{ get_final_rate_code_description("ratecodeid") }} as final_rate_code_description,  
    {{ get_LPEP_provider("venderid") }} as LPEP_provider    

  from tripdata
  where rn = 1

)
select * from renamed 





