{% macro ensure_schema_exists(
    custom_schema_name,
    database = target.database
) -%}
CREATE SCHEMA IF NOT EXISTS {{database}}.{{ get_schema_name(custom_schema_name) }};
{%- endmacro %}
