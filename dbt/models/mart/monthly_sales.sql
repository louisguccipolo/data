-- depends_on: {{ ref('err_cast') }}
with months as (
    select distinct month_start as month
    from {{ ref('dim_date') }}
    where month_start >= date '2016-09-01' and month_start < date '2018-09-01'
), orders_by_month as (
    -- Меры уже агрегированы к заказу в DDS: заказы без позиций не теряются.
    select date_trunc('month', o.order_purchase_ts)::date as month,
        count(*) as orders_all_count,
        count(*) filter (where s.is_revenue) as orders_delivered_count,
        sum(o.items_price) filter (where s.is_revenue) as revenue_amount
    from {{ ref('fct_order__active') }} o
    left join {{ ref('dim_order_status') }} s on s.order_status = o.order_status and s.is_current
    where o.order_purchase_ts >= timestamp '2016-09-01'
        and o.order_purchase_ts < timestamp '2018-09-01'
    group by 1
)
select m.month::date as month,
    coalesce(o.orders_all_count, 0)::bigint as orders_all_count,
    coalesce(o.orders_delivered_count, 0)::bigint as orders_delivered_count,
    coalesce(o.revenue_amount, 0)::numeric(18,2) as revenue_amount
from months m left join orders_by_month o using (month)
