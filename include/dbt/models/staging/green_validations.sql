{{ config(
    materialized='table'
) }}

with source_data as (
    select * 
    from {{ ref('green_preprocessing') }}
)

SELECT
    *,
    {{ generate_case_columns('green_warn_rules') }} AS quality_warn
    --,
    --{{ generate_case_columns('green_drop_rules') }} AS quality_drop
FROM source_data


-- Add debug output to check what the macro returns
{% set result = generate_case_columns('green_warn_rules') %}
{{ log(result, info=True) }}

{% set result_drop = generate_case_columns('green_drop_rules') %}
{{ log(result_drop, info=True) }}
