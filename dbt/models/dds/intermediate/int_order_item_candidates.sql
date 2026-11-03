-- depends_on: {{ ref('err_cast') }}
with eff as ({{ dds_latest('eff_order_item', 'order_item_hk') }}),
sat as ({{ dds_latest('sat_order_item', 'order_item_hk') }})
select l.*, coalesce(e.is_deleted, false) as is_deleted, e.load_dts as eff_load_dts,
    s.load_dts as sat_load_dts, s.price, s.freight_value, s.shipping_limit_date as shipping_limit_ts,
    case when e.is_deleted = false then 1 when e.is_deleted is null then 2 else 3 end as priority
from {{ ref('link_order_item') }} l
left join eff e using (order_item_hk) left join sat s using (order_item_hk)
where l.load_dts <= (select as_of_dts from {{ ref('int_as_of') }}) and l.order_hk <> repeat('0',32) and l.order_item_hk <> repeat('0',32)
