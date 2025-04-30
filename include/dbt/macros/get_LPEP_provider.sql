{#
    This is indicating the LPEP provider that provided the record.
#}

{% macro get_LPEP_provider(vendorid) -%}
    case {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} 
        when 1 then 'Creative Mobile Technologies and LLC'
        when 2 then 'VeriFone Inc.' 
        else 'EMPTY' 

    end


{%- endmacro %}