{% macro hashdiff(columns) -%}
md5({% for c in columns %}coalesce({{ c }}::text, ''){% if not loop.last %} || '||' || {% endif %}{% endfor %})::char(32)
{%- endmacro %}
