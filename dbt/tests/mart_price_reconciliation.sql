{{ config(severity='error') }}
select 'units' as check_name from {{ ref('product_price_history') }}
having coalesce(sum(unit_count), 0) <> (
    select count(*) from {{ ref('fct_order_item__active') }} i
    join {{ ref('fct_order__active') }} o on o.order_hk = i.order_hk
    join {{ ref('dim_order_status') }} s on s.order_status = o.order_status and s.is_current and s.is_revenue
)
union all
select product_id from {{ ref('product_price_history') }} group by product_id
having min(distinct_price_count) <> count(distinct price) or max(distinct_price_count) <> count(distinct price)
    or min(product_unit_count) <> sum(unit_count) or max(product_unit_count) <> sum(unit_count)
