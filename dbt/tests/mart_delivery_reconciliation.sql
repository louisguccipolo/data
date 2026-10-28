{{ config(severity='error') }}
select 'orders' as check_name from {{ ref('delivery_by_state') }}
having coalesce(sum(orders_count), 0) <> (select count(*) from {{ ref('fct_order__active') }})
union all
select 'measured' from {{ ref('delivery_by_state') }}
having coalesce(sum(delivered_measured_count), 0) <> (
    select count(*) from {{ ref('fct_order__active') }} o
    join {{ ref('dim_order_status') }} s on s.order_status = o.order_status and s.is_current and s.is_revenue
    where o.order_delivered_customer_ts is not null
)
