{# Input contract: each observed parent/batch contains its COMPLETE active set.
   Partial delta rows and empty sets cannot be distinguished by the current EL.
   See .plans/ods-implementation.md before connecting a CDC feed. #}
{% macro multi_active_sat(source_model) %}
with rows as (
    select distinct order_hk, payment_sequential, payment_type,
        payment_installments, payment_value, payment_row_hashdiff, load_dts, record_source
    from {{ ref(source_model) }}
), sets as (
    select order_hk, load_dts,
        md5(string_agg(payment_row_hashdiff::text, '||' order by payment_sequential nulls first, payment_row_hashdiff))::char(32) as payment_hashdiff
    from rows
    group by order_hk, load_dts
), ordered as (
    select *, lag(payment_hashdiff) over (partition by order_hk order by load_dts) as previous_hashdiff
    from sets
), changed as (
    select * from ordered where payment_hashdiff is distinct from previous_hashdiff
)
select r.order_hk, c.payment_hashdiff, r.payment_sequential,
    r.payment_type, r.payment_installments, r.payment_value, r.load_dts, r.record_source
from changed c
join rows r using (order_hk, load_dts)
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t where t.order_hk = r.order_hk and t.load_dts = r.load_dts
)
{% endif %}
{% endmacro %}
