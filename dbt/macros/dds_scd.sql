{% macro dds_scd(entity, attributes) %}
with states as (
    select *, jsonb_build_array({{ attributes.keys() | join(', ') }}, is_deleted, is_incomplete) as state
    from {{ ref('int_' ~ entity ~ '_versions') }}
), marked as (
    select *, lag(state) over (partition by {{ entity }}_hk order by valid_from) as previous from states
), versions as (
    select *, lead(valid_from, 1, 'infinity'::timestamptz) over (
        partition by {{ entity }}_hk order by valid_from) as valid_to
    from marked where state is distinct from previous
)
select {{ dds_sk(entity ~ '_hk', 'valid_from') }} as {{ entity }}_sk,
    {{ entity }}_hk, valid_from, valid_to, valid_to = 'infinity'::timestamptz as is_current,
    is_deleted, is_incomplete, false as is_unknown,
    {{ attributes.keys() | join(', ') }}
from versions
union all
select repeat('0',32)::char(32), repeat('0',32)::char(32), '-infinity'::timestamptz,
    'infinity'::timestamptz, true, false, false, true,
    {% for name, value in attributes.items() %}{{ value }}{% if not loop.last %}, {% endif %}{% endfor %}
{% endmacro %}
