-- depends_on: {{ ref('err_cast') }}
with orders as ({{ dds_latest('sat_order', 'order_hk') }}),
status as ({{ dds_latest('sat_order_status', 'order_hk') }}),
eff as ({{ dds_latest('eff_order', 'order_hk') }})
select h.order_hk, h.order_id, o.order_purchase_timestamp as order_purchase_ts,
    o.order_estimated_delivery_date as order_estimated_delivery_ts,
    s.order_status, s.order_approved_at as order_approved_ts,
    s.order_delivered_carrier_date as order_delivered_carrier_ts,
    s.order_delivered_customer_date as order_delivered_customer_ts,
    coalesce(e.is_deleted, false) as is_deleted, o.order_hk is null as is_incomplete
from {{ ref('hub_order') }} h
left join orders o using (order_hk) left join status s using (order_hk) left join eff e using (order_hk)
where h.load_dts <= (select as_of_dts from {{ ref('int_as_of') }}) and h.order_hk <> repeat('0',32)
