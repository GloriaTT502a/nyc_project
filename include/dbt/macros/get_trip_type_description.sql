{#
    This macro returns the description of the trip_type 
#}


{% macro get_trip_type_description(trip_type) -%} 

    case {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} 
        when 1 then 'Street-hail' 
        when 2 then 'Dispatch' 
        else 'EMPTY' 
    end 

{%- endmacro %}