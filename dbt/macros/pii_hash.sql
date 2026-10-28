{# sha256 с солью: для джойнов/дедупа по email без раскрытия самого email #}
{% macro pii_hash(expr) -%}
    encode(sha256(convert_to('{{ var("pii_salt") }}' || {{ expr }}, 'UTF8')), 'hex')
{%- endmacro %}
