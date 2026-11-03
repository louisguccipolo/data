-- depends_on: {{ ref('err_cast') }}
with sales as (
    select i.product_hk, o.order_purchase_ts::date as sale_date, i.price,
        count(*)::bigint as unit_count, count(distinct i.order_hk)::bigint as orders_count
    from {{ ref('fct_order_item__active') }} i
    join {{ ref('fct_order__active') }} o on o.order_hk = i.order_hk
    join {{ ref('dim_order_status') }} s on s.order_status = o.order_status and s.is_current and s.is_revenue
    group by i.product_hk, o.order_purchase_ts::date, i.price
), products as (
    select product_hk, count(distinct price)::bigint as distinct_price_count,
        sum(unit_count)::bigint as product_unit_count
    from sales group by product_hk
)
select coalesce(p.product_id, '__unknown__')::text as product_id, s.sale_date::date as sale_date,
    s.price::numeric(18,2) as price, s.unit_count, s.orders_count,
    t.distinct_price_count, t.product_unit_count
from sales s join products t using (product_hk)
left join {{ ref('dim_product') }} p on p.product_hk = s.product_hk and p.is_current
