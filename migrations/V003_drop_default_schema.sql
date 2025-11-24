{% macro V003_drop_default_schema(
        database = target.database,
        schema_prefix = target.schema
) -%}

DROP SCHEMA IF EXISTS  {{database}}.{{schema_prefix}};

{%- endmacro %}