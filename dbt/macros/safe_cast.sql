{# Cast the selected text OUTSIDE CASE: an invalid literal in a cast inside
   THEN may be constant-folded by PostgreSQL before the CASE guard runs. #}
{% macro safe_cast(column, data_type) -%}
(case when pg_input_is_valid(nullif(trim({{ column }}::text), ''), '{{ data_type }}')
then nullif(trim({{ column }}::text), '') end)::{{ data_type }}
{%- endmacro %}
