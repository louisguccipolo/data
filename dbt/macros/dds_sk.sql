{% macro dds_sk(hk, valid_from) -%}
md5({{ hk }} || '||' || to_char({{ valid_from }} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US'))::char(32)
{%- endmacro %}
