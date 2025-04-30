{% macro green_trip_warn_rules() %}
  {% set rules = [
    {'name': 'pickup_datetime_null', 'condition': 'pickup_datetime IS NULL'},
    {'name': 'fare_amount_negative', 'condition': 'fare_amount < 0'},
    {'name': 'passenger_count_zero', 'condition': 'passenger_count = 0'},
    {'name': 'trip_distance_negative', 'condition': 'trip_distance < 0'},
    {'name': 'total_amount_negative', 'condition': 'total_amount < 0'}
  ] %}
  {{ return(rules) }}
{% endmacro %}

{% macro green_trip_drop_rules() %}
  {% set rules = [
    {'name': 'pickup_datetime_invalid', 'condition': 'pickup_datetime > CURRENT_TIMESTAMP()'},
    {'name': 'dropoff_datetime_invalid', 'condition': 'dropoff_datetime > CURRENT_TIMESTAMP()'},
    {'name': 'pickup_dropoff_invalid', 'condition': 'pickup_datetime > dropoff_datetime'},
    {'name': 'fare_amount_null', 'condition': 'fare_amount IS NULL'},
    {'name': 'total_amount_null', 'condition': 'total_amount IS NULL'}
  ] %}
  {{ return(rules) }}
{% endmacro %}

{% macro generate_validation_array(rules, array_name) %}
  {% set conditions = [] %}
  {% set case_statements = [] %}
  {% for rule in rules %}
    {% do conditions.append(rule.condition) %}
    {% do case_statements.append("CASE WHEN " ~ rule.condition ~ " THEN '" ~ rule.name ~ "' END") %}
  {% endfor %}
  {% set combined_condition = conditions | join(' OR ') %}
  CASE
    {% if combined_condition %}
    WHEN {{ combined_condition }}
    {% else %}
    WHEN FALSE
    {% endif %}
    THEN (
      SELECT ARRAY_AGG(rule IGNORE NULLS)
      FROM UNNEST(ARRAY<STRING>[
        {% for case_statement in case_statements %}
          {{ case_statement }}{% if not loop.last %},{% endif %}
        {% endfor %}
      ]) AS rule
    )
    {% if array_name == 'quality_drop' %}
    ELSE ARRAY<STRING>[]
    {% else %}
    ELSE NULL
    {% endif %}
  END AS {{ array_name }}
{% endmacro %}