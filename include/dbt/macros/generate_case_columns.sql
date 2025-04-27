{#
    Generate SQL to identify the category of violation of rules 
#}

{% macro generate_case_columns(rule_type) %}
    {% set rules = var(rule_type) %}
    {% set case_clauses_warn = [] %}
    {% set case_clauses_drop = [] %}
    
    {% for rule in rules %}
        {% set clause %}
            case when {{ rule.condition }} then '{{ rule.name }}' else null end
        {% endset %}
        
        {% if rule_type == 'green_warn_rules' %}
            {% set case_clauses_warn = case_clauses_warn + [clause] %}
        {% else %}
            {% set case_clauses_drop = case_clauses_drop + [clause] %}
        {% endif %}
    {% endfor %}
    
    {% set warn_array = case_clauses_warn | join(', ') %}
    {% set drop_array = case_clauses_drop | join(', ') %}
    
    {% set warn_condition = 'ARRAY[' ~ warn_array ~ ']' %}
    {% set drop_condition = 'ARRAY[' ~ drop_array ~ ']' %}
    
    {{ return(warn_condition ~ ',' ~ drop_condition) }}
{% endmacro %}
