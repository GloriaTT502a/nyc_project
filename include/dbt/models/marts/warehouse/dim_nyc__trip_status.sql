{{ config(
    materialized='table',
    
) }}

WITH unioned_data AS 
(
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['payment_type', 'vendorid', 'store_and_fwd_flag', 'ratecodeid']) }} as trip_status_id,
        payment_type, 
        9999 as trip_type, 
        vendorid, 
        store_and_fwd_flag, 
        ratecodeid, 
        payment_type_description, 
        final_rate_code_description, 
        LPEP_provider  
    FROM 
        {{ ref('stg_nyc__yellow_valid_data') }} 
    
    UNION ALL 
    
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['payment_type', 'trip_type', 'vendorid', 'store_and_fwd_flag', 'ratecodeid']) }} as trip_status_id,
        payment_type, 
        trip_type, 
        vendorid, 
        store_and_fwd_flag, 
        ratecodeid, 
        payment_type_description, 
        final_rate_code_description, 
        LPEP_provider  
    FROM 
        {{ ref('stg_nyc__green_valid_data') }}
), 
deduplicated AS 
( 
    SELECT 
        trip_status_id, 
        payment_type, 
        trip_type, 
        vendorid, 
        store_and_fwd_flag, 
        ratecodeid, 
        payment_type_description, 
        final_rate_code_description, 
        LPEP_provider  
    FROM 
        unioned_data 
    GROUP BY 
        trip_status_id, 
        payment_type, 
        trip_type, 
        vendorid, 
        store_and_fwd_flag, 
        ratecodeid, 
        payment_type_description, 
        final_rate_code_description, 
        LPEP_provider 
) 
SELECT * FROM deduplicated 