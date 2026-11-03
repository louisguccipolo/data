{#
  Record tracking satellite DV 2.0: «в батч load_dts ключ был в источнике».
  Без атрибутов — по строке на ключ × батч.

  Источник — staging снимка ключей (stg_<таблица>__keys): EL на каждом прогоне выгружает
  все бизнес-ключи таблицы, а не только изменившиеся строки (спека §4.2). Только по полному
  снимку видно, что ключ пропал, — на этом строится eff_sat.
#}
{% macro rts(source_model, hash_key) -%}
select distinct {{ hash_key }}, load_dts, record_source
from {{ ref(source_model) }}
{%- if is_incremental() %}
where load_dts > (select coalesce(max(load_dts), '-infinity') from {{ this }})
{%- endif %}
{%- endmacro %}
