{#
  Сателлит DV 2.0: только вставка, новая строка — когда hashdiff ключа изменился.

  Каждая строка пачки сравнивается с предыдущей версией того же ключа (lag),
  первая строка пачки — с последней версией в сателлите. Поэтому результат не зависит
  от того, сколько батчей пришло за прогон: --full-refresh по всему raw даёт ту же
  историю, что ежедневные прогоны, а повторы (перекрытие водяного знака,
  перезаливка источника) схлопываются.

  hash_key, hashdiff, load_dts, record_source считает staging (спека §6).
  Модель: {{ config(materialized='incremental') }} {{ sat('stg_orders', 'order_hk', ['order_status', ...]) }}
#}
{% macro sat(source_model, hash_key, payload, hashdiff='hashdiff') -%}
with src as (
    select distinct
        {{ hash_key }}, {{ hashdiff }}, {{ payload | join(', ') }}, load_dts, record_source
    from {{ ref(source_model) }}
    {%- if is_incremental() %}
    where load_dts > (select coalesce(max(load_dts), '-infinity') from {{ this }})
    {%- endif %}
),

{%- if is_incremental() %}
cur as (   -- последняя версия в сателлите, только по ключам из пачки
    select distinct on ({{ hash_key }}) {{ hash_key }}, {{ hashdiff }}
    from {{ this }}
    where {{ hash_key }} in (select {{ hash_key }} from src)
    order by {{ hash_key }}, load_dts desc
),
{%- endif %}

ordered as (
    select src.*, lag({{ hashdiff }}) over (partition by {{ hash_key }} order by load_dts) as _prev_hashdiff
    from src
)

select o.{{ hash_key }}, o.{{ hashdiff }}, {% for c in payload %}o.{{ c }}, {% endfor %}o.load_dts, o.record_source
from ordered o
{%- if is_incremental() %}
left join cur on cur.{{ hash_key }} = o.{{ hash_key }}
where o.{{ hashdiff }} is distinct from coalesce(o._prev_hashdiff, cur.{{ hashdiff }})
{%- else %}
where o.{{ hashdiff }} is distinct from o._prev_hashdiff
{%- endif %}
{%- endmacro %}
