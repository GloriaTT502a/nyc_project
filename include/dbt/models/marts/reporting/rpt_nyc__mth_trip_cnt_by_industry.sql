{{ config(
    materialized='incremental',
    unique_key=['yearmonth', 'industry_type'],
    pre_hook=[
        "delete from {{ this }} where yearmonth BETWEEN 202101 AND 202112"
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
    WHERE 
        yearmonth BETWEEN 201901 AND 201912 
    OR 
        yearmonth BETWEEN 202101 AND 202112 

    UNION ALL 

    SELECT 
        yearmonth, 
        industry_type_id, 
        pickup_locationid, 
        dropoff_locationid, 
        trip_count 
    FROM 
        {{ ref('fct_nyc__green_yellow_trip_data') }}  
    WHERE 
        yearmonth BETWEEN 201901 AND 201912 
    OR 
        yearmonth BETWEEN 202101 AND 202112
), 
data_2019 AS 
(
    SELECT 
        yearmonth,
        mod(yearmonth,100) as month,  
        industry_type_id, 
        SUM(trip_count) AS TRIP_CNT
    FROM 
        unioned_data  
    WHERE yearmonth BETWEEN 201901 AND 201912  
    GROUP BY 
        yearmonth, 
        industry_type_id 
 
), 
cur_data AS 
(
    SELECT 
        yearmonth, 
        mod(yearmonth,100) as month,  
        industry_type_id, 
        sum(trip_count) AS TRIP_CNT
    FROM 
        unioned_data  
    WHERE yearmonth BETWEEN 2021*100+1 AND 2021*100+12  
    GROUP BY 
        yearmonth, 
        industry_type_id  
), 
nyc_drop AS 
(
    SELECT 
        yearmonth, 
        industry_type_id, 
        AVG(CASE 
            WHEN zo.Borough IN ('Queens', 'Bronx', 'Brooklyn', 'Manhattan', 'Staten Island') 
            THEN 1.0 
            ELSE 0.0 
        END) AS nyc_dropoff_trip_rate
    FROM 
        unioned_data ud 
    JOIN 
        {{ ref('dim_nyc__zones') }} zo 
    ON ud.dropoff_locationid = zo.locationid 
    WHERE yearmonth BETWEEN 2021*100+1 AND 2021*100+12  
    GROUP BY 
        yearmonth, 
        industry_type_id  
), 
final AS 
(
    SELECT 
        cur_data.yearmonth, 
        ind_typ.industry_type, 
        data_2019.TRIP_CNT / cur_data.TRIP_CNT AS trip_relv_prepan, 
        nyc_dropoff_trip_rate 
    FROM 
        data_2019 
    JOIN 
        cur_data 
    ON data_2019.month = cur_data.month AND 
        data_2019.industry_type_id = cur_data.industry_type_id  
    JOIN 
        nyc_drop 
    ON cur_data.yearmonth = nyc_drop.yearmonth AND 
        cur_data.industry_type_id = nyc_drop.industry_type_id 
    JOIN {{ ref('dim_nyc__industry_type') }} ind_typ 
    ON cur_data.industry_type_id = ind_typ.industry_type_id 
) 


SELECT * FROM final 



