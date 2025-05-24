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
        SUM(trip_count) AS nyc_dropoff_trip
    FROM 
        unioned_data ud 
    JOIN 
        {{ ref('dim_nyc__zones') }} zo 
    ON ud.dropoff_locationid = zo.locationid 
    WHERE yearmonth BETWEEN 2021*100+1 AND 2021*100+12  
    AND Borough IN ('Queens', 'Bronx', 'Brooklyn', 'Manhattan', 'Staten Island') 
    GROUP BY 
        yearmonth, 
        industry_type_id  
), 
months AS 
(
    SELECT
        DATE '2021-01-01' + INTERVAL m MONTH AS month_start,
        DATE_TRUNC(DATE '2021-01-01' + INTERVAL m MONTH, MONTH) AS month_first_day
    FROM UNNEST(GENERATE_ARRAY(0, 11)) AS m
), 
min_cnt AS 
(
    SELECT
        CAST(FORMAT_DATE('%Y%m', month_start) AS INTEGER) AS yearmonth,
        TIMESTAMP_DIFF(
            DATE_ADD(month_first_day, INTERVAL 1 MONTH),
            month_first_day,
            MINUTE
        ) AS minutes_in_month
    FROM months
), 
final AS 
(
    SELECT 
        cur_data.yearmonth, 
        ind_typ.industry_type, 
        cur_data.TRIP_CNT AS trip_count, 
        data_2019.TRIP_CNT AS prepan_count, 
        cur_data.TRIP_CNT / data_2019.TRIP_CNT AS trip_relv_prepan, 
        min_cnt.minutes_in_month AS minutes_in_month, 
        nyc_drop.nyc_dropoff_trip 
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
    JOIN min_cnt 
    ON cur_data.yearmonth = min_cnt.yearmonth 
) 
SELECT * FROM final 



