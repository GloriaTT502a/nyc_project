{{ config(
    materialized='table',
    
) }} 

WITH unioned_data AS 
(
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'affiliated_base_number', 'sr_flag']) }} as base_num_sr_id,
        dispatching_base_num, 
        affiliated_base_number, 
        'NA' AS hvfhs_license_num, 
        sr_flag 
    FROM 
        {{ ref('stg_nyc__fhv_valid_data') }} 
    
    UNION ALL 
    
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'hvfhs_license_num', 'sr_flag']) }} as base_num_sr_id,
        dispatching_base_num, 
        'NA' AS affiliated_base_number, 
        hvfhs_license_num, 
        sr_flag 
    FROM 
        {{ ref('stg_nyc__fhvhv_valid_data') }}
), 
deduplicated AS 
( 
    SELECT 
        base_num_sr_id, 
        dispatching_base_num, 
        affiliated_base_number, 
        hvfhs_license_num, 
        sr_flag 
    FROM 
        unioned_data 
    GROUP BY 
        base_num_sr_id, 
        dispatching_base_num, 
        affiliated_base_number, 
        hvfhs_license_num, 
        sr_flag
) 
SELECT * FROM deduplicated 