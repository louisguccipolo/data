{% macro hub(sources, hash_key, business_key, ghost=false) -%}
with src as (
{% for source_model in sources %}
select {{ hash_key }}, {{ business_key }}, load_dts, record_source from {{ ref(source_model) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
{% if ghost %}
union all select repeat('0',32)::char(32), null::text, '-infinity'::timestamptz, 'SYSTEM'
{% endif %}
), first_seen as (
select distinct on ({{ hash_key }}) {{ hash_key }}, {{ business_key }}, load_dts, record_source
from src order by {{ hash_key }}, load_dts, record_source, {{ business_key }}
)
select * from first_seen s
{% if is_incremental() %}
where not exists (select 1 from {{ this }} t where t.{{ hash_key }} = s.{{ hash_key }})
{% endif %}
{%- endmacro %}
