{% macro dds_bound_version(entity) -%}
{# SCD intervals are contiguous. Extend the first interval back to -infinity:
   before the HK first appears, use its first complete version on the slice,
   or its first incomplete version if no complete version exists yet.
   An incomplete interval also advances to that first complete version. #}
select d.*,
    case when row_number() over (partition by d.{{ entity }}_hk order by d.valid_from, d.{{ entity }}_sk) = 1
        then '-infinity'::timestamptz else d.valid_from end as bound_valid_from,
    coalesce(f.{{ entity }}_sk, d.{{ entity }}_sk) as bound_sk
from {{ ref('dim_' ~ entity) }} d
left join (
    select distinct on ({{ entity }}_hk) {{ entity }}_hk, {{ entity }}_sk
    from {{ ref('dim_' ~ entity) }}
    where not is_incomplete
    order by {{ entity }}_hk, valid_from, {{ entity }}_sk
) f on f.{{ entity }}_hk = d.{{ entity }}_hk and d.is_incomplete
{%- endmacro %}
