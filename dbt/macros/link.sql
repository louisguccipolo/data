{% macro link(source_model, hash_key, keys) -%}
with first_seen as (
select distinct on ({{ hash_key }}) {{ hash_key }}, {{ keys|join(', ') }}, load_dts, record_source
from {{ ref(source_model) }} order by {{ hash_key }}, load_dts, record_source
)
select * from first_seen s
{% if is_incremental() %}
where not exists (select 1 from {{ this }} t where t.{{ hash_key }} = s.{{ hash_key }})
{% endif %}
{%- endmacro %}
