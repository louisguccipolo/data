{# Composite order is part of the persistent key contract; all-empty maps to ghost. #}
{% macro hash_key(columns) -%}
(case when {% for c in columns %}nullif(trim({{ c }}::text), '') is null{% if not loop.last %} and {% endif %}{% endfor %}
then repeat('0', 32) else md5({% for c in columns %}coalesce(upper(nullif(trim({{ c }}::text), '')), ''){% if not loop.last %} || '||' || {% endif %}{% endfor %}) end)::char(32)
{%- endmacro %}
