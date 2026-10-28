-- depends_on: {{ ref('err_cast') }}
with items as (
    select order_hk, count(*) as items_count, sum(price) as items_price,
        sum(freight_value) as items_freight, sum(price + freight_value) as items_value
    from {{ ref('fct_order_item') }} where not is_deleted group by 1
), payments as (
    select order_hk, count(*) as payments_count, sum(payment_value) as payment_value
    from {{ ref('fct_payment') }} where not is_deleted group by 1
), orders as (
    select o.*, coalesce(c.bound_sk, repeat('0',32)::char(32)) as customer_sk,
        coalesce(c.customer_hk, repeat('0',32)::char(32)) as customer_hk,
        p.customer_id, p.customer_zip_code_prefix, p.customer_city, p.customer_state,
        coalesce(t.order_status, '__unknown__') as status_code, coalesce(t.is_revenue, false) as is_revenue,
        coalesce(d.date_key, -1) as order_purchase_date_key,
        coalesce(i.items_count, 0) as items_count, coalesce(i.items_price, 0) as items_price,
        coalesce(i.items_freight, 0) as items_freight, coalesce(i.items_value, 0) as items_value,
        coalesce(v.payments_count, 0) as payments_count, coalesce(v.payment_value, 0) as payment_value
    from {{ ref('int_order_on_slice') }} o
    left join {{ ref('int_order_profile_on_slice') }} p using (order_hk)
    left join ({{ dds_bound_version('customer') }}) c on c.customer_hk = p.customer_hk
        and c.bound_valid_from <= p.bind_dts and p.bind_dts < c.valid_to
    left join {{ ref('dim_order_status') }} t on t.order_status = o.order_status
    left join {{ ref('dim_date') }} d on d.date = o.order_purchase_ts::date
    left join items i using (order_hk) left join payments v using (order_hk)
)
select order_id, order_hk, customer_sk, customer_id, customer_zip_code_prefix, customer_city, customer_state,
    status_code as order_status, order_purchase_ts, order_approved_ts, order_delivered_carrier_ts,
    order_delivered_customer_ts, order_estimated_delivery_ts, order_purchase_date_key,
    items_count, items_price, items_freight, items_value, payments_count, payment_value, is_revenue,
    case when order_approved_ts >= order_purchase_ts
        then extract(epoch from (order_approved_ts - order_purchase_ts)) / 86400 end as approve_days,
    case when order_delivered_carrier_ts >= order_purchase_ts
        then extract(epoch from (order_delivered_carrier_ts - order_purchase_ts)) / 86400 end as to_carrier_days,
    case when order_delivered_customer_ts >= order_purchase_ts
        then extract(epoch from (order_delivered_customer_ts - order_purchase_ts)) / 86400 end as to_customer_days,
    order_delivered_customer_ts::date - order_estimated_delivery_ts::date as delivery_delay_days,
    order_delivered_customer_ts::date > order_estimated_delivery_ts::date as is_late,
    status_code = 'delivered' and order_delivered_customer_ts is null as is_delivered_without_date,
    -- canceled/unavailable могли закрыться до подтверждения: пустая дата у них не аномалия
    status_code in ('invoiced', 'processing', 'shipped', 'delivered') and order_approved_ts is null as is_approved_missing,
    coalesce(order_delivered_carrier_ts < order_purchase_ts, false) as is_carrier_before_purchase,
    is_deleted, is_incomplete, customer_hk
from orders
