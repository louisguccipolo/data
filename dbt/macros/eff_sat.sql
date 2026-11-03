{#
  Effectivity satellite: история «ключ жив / удалён в источнике», выводится из rts.

  Каждый батч rts — полный снимок ключей. Ключа нет в снимке, а раньше был → is_deleted = true;
  вернулся → снова false. Хранятся только смены признака, по тому же правилу, что в sat
  (сравнение с предыдущей версией), поэтому --full-refresh даёт ту же историю.
  С CDC признак будет приходить из событий op = 'd'; модель и потребители в dds не меняются.
#}
{% macro eff_sat(rts_model, hash_key) -%}
with rts as (
    select {{ hash_key }}, load_dts, record_source from {{ ref(rts_model) }}
),

batches as (
    select load_dts, min(record_source) as record_source
    from rts
    {%- if is_incremental() %}
    where load_dts > (select coalesce(max(load_dts), '-infinity') from {{ this }})
    {%- endif %}
    group by load_dts
),

keys as (   -- ключ отслеживается с первого батча, где он появился
    select {{ hash_key }}, min(load_dts) as first_seen from rts group by {{ hash_key }}
),

grid as (
    select k.{{ hash_key }}, b.load_dts, b.record_source, r.{{ hash_key }} is null as is_deleted
    from keys k
    join batches b on b.load_dts >= k.first_seen
    left join rts r on r.{{ hash_key }} = k.{{ hash_key }} and r.load_dts = b.load_dts
),

{%- if is_incremental() %}
cur as (
    select distinct on ({{ hash_key }}) {{ hash_key }}, is_deleted
    from {{ this }}
    order by {{ hash_key }}, load_dts desc
),
{%- endif %}

ordered as (
    select grid.*, lag(is_deleted) over (partition by {{ hash_key }} order by load_dts) as _prev_is_deleted
    from grid
)

select o.{{ hash_key }}, o.is_deleted, o.load_dts, o.record_source
from ordered o
{%- if is_incremental() %}
left join cur on cur.{{ hash_key }} = o.{{ hash_key }}
where o.is_deleted is distinct from coalesce(o._prev_is_deleted, cur.is_deleted)
{%- else %}
where o.is_deleted is distinct from o._prev_is_deleted
{%- endif %}
{%- endmacro %}
