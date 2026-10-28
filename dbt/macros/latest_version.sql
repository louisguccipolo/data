{# raw хранит все версии строк — берём последнюю по updated_at для каждого ключа. #}
{% macro latest_version(source_table, key) -%}
    select *
    from (
        select
            src.*,
            row_number() over (partition by {{ key }} order by updated_at desc, _loaded_at desc) as _rn
        from {{ source('raw', source_table) }} as src
    ) as t
    where _rn = 1
{%- endmacro %}

{# sha256 с солью: для джойнов/дедупа по email без раскрытия самого email #}
{% macro pii_hash(expr) -%}
    encode(sha256(convert_to('{{ var("pii_salt") }}' || {{ expr }}, 'UTF8')), 'hex')
{%- endmacro %}

{% macro age_band(birth_year_expr) -%}
    case
        when {{ birth_year_expr }} is null then 'unknown'
        when extract(year from current_date) - {{ birth_year_expr }} < 25 then '<25'
        when extract(year from current_date) - {{ birth_year_expr }} < 35 then '25-34'
        when extract(year from current_date) - {{ birth_year_expr }} < 45 then '35-44'
        when extract(year from current_date) - {{ birth_year_expr }} < 55 then '45-54'
        else '55+'
    end
{%- endmacro %}
