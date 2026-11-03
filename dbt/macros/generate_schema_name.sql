{# Схемы ровно как в конфиге (ods, dds, mart...), без префикса target-схемы. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name is not none else target.schema }}
{%- endmacro %}
