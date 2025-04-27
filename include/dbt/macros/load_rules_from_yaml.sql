{% macro load_rules_from_yaml(file_path) %}
  {% set yaml_content = read_file(file_path) %}
  {% set rules = fromyaml(yaml_content) %}
  {{ return(rules) }}
{% endmacro %}