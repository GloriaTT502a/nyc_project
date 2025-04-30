{{ config(
    materialized='table',
    alias='green_validations'
) }}

WITH raw_data AS (
    SELECT
        * 
    FROM {{ ref('stg_green_tripdata') }}
),
validated_data AS (
    SELECT
        *,
        {{ generate_validation_array(green_trip_warn_rules(), 'quality_warn') }},
        {{ generate_validation_array(green_trip_drop_rules(), 'quality_drop') }},
        ARRAY<STRING>[] AS fixes_applied  -- Initialize as empty array
    FROM raw_data
)
SELECT *
FROM validated_data