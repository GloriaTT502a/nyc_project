{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
    row_number() over(partition by pickup_datetime, dropOff_datetime) as rn
  from {{ source('base','raw_fhvhv_trips') }}
  --where vendorid is not null 
), 
renamed as (
  select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['pickup_datetime', 'dropOff_datetime']) }} as tripid, 
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,  
    
    -- timestamps 
    cast(pickup_datetime as timestamp) as pickup_datetime, 
    cast(dropOff_datetime as timestamp) as dropoff_datetime, 

    -- yearmonth 
    yearmonth as yearmonth, 

    -- trip info
    dispatching_base_num as dispatching_base_num, 
    hvfhs_license_num as hvfhs_license_num, 
    
    -- payment info
    SR_Flag as sr_flag      

  from tripdata
  where rn = 1

)
select * from renamed 



