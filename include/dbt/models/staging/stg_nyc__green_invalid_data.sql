{{ config(
    materialized='table',
    
) }}

WITH invalid_data as (
    SELECT
        * 
    FROM {{ ref('stg_nyc__green_validations') }}
    WHERE ARRAY_LENGTH(quality_drop) > 0  
    OR ARRAY_LENGTH(quality_warn) > 0-- Only keep records with non-empty quality_drop 
) 
SELECT * FROM invalid_data 