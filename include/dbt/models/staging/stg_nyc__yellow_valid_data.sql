{{ config(
    materialized='table',
) }}

WITH valid_data AS 
(
    SELECT
        * 
    FROM {{ ref('stg_nyc__yellow_validations') }}
    WHERE ARRAY_LENGTH(quality_drop) = 0 
    AND ARRAY_LENGTH(quality_warn) = 0 -- Only keep records with empty quality_drop
)
SELECT * FROM valid_data 