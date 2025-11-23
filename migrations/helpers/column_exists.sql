{% macro column_exists(model, column) -%}
    {{ return(adapter.dispatch('column_exists')(model, column)) }}
{%- endmacro %}

{% macro default__column_exists(model, column) %}

{% set column_exists_query %}
SELECT 1 as column_exists
FROM {{ model.database }}.INFORMATION_SCHEMA.COLUMNS col
WHERE TABLE_CATALOG = '{{ model.database | upper }}'
  and TABLE_SCHEMA = '{{ model.schema | upper }}'
  and TABLE_NAME = '{{ model.identifier | upper }}'
  and COLUMN_NAME ILIKE '{{column}}';
{% endset %}

{{ log('Checking existence of column '~column~' in model '~model~'.', info=False) }}

{% if execute %}
    {% set results = run_query(column_exists_query) %}
    {{ return(results.rows|length > 0) }}
{% endif %}

{{ return(false) }}

{% endmacro %}

{% macro fabric__column_exists(model, column) %}

{% set column_exists_query %}
SELECT 1 as column_exists
FROM {{ model.database }}.INFORMATION_SCHEMA.COLUMNS col
WHERE TABLE_CATALOG = '{{ model.database }}'
  and TABLE_SCHEMA = '{{ model.schema }}'
  and TABLE_NAME = '{{ model.identifier }}'
  and COLUMN_NAME LIKE '{{column}}';
{% endset %}

{{ log('[Fabric] Checking existence of column '~column~' in model '~model~'.', info=False) }}

{% if execute %}
    {% set results = run_query(column_exists_query) %}
    {{ return(results.rows|length > 0) }}
    {% endif %}

    {{ return(false) }}

{% endmacro %}