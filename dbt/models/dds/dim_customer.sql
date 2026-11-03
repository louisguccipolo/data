-- depends_on: {{ ref('err_cast') }}
with stats as (
    select p.customer_hk, min(o.order_purchase_ts) as first_order_purchase_ts, count(*) as orders_count
    from {{ ref('int_order_on_slice') }} o join {{ ref('int_order_profile_on_slice') }} p using (order_hk)
    where not o.is_deleted group by 1
)
select {{ dds_sk('h.customer_hk', 'h.load_dts') }} as customer_sk, h.customer_hk,
    h.load_dts as valid_from, 'infinity'::timestamptz as valid_to,
    true as is_current, false as is_deleted, false as is_incomplete, false as is_unknown,
    h.customer_unique_id, s.first_order_purchase_ts,
    date_trunc('month', s.first_order_purchase_ts)::date as first_order_month,
    coalesce(s.orders_count, 0)::bigint as orders_count
from {{ ref('hub_customer') }} h left join stats s using (customer_hk)
where h.load_dts <= (select as_of_dts from {{ ref('int_as_of') }}) and h.customer_hk <> repeat('0',32)
union all
select repeat('0',32)::char(32), repeat('0',32)::char(32), '-infinity'::timestamptz,
    'infinity'::timestamptz, true, false, false, true, null::text, null::timestamp, null::date, 0::bigint
