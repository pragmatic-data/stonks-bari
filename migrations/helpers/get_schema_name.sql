{% macro get_schema_name(custom_schema_name) %}
  {% do return(generate_schema_name(custom_schema_name, none) | trim ) %}
{% endmacro %}