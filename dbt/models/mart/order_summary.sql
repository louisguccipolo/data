-- depends_on: {{ ref('err_cast') }}
-- Сначала меры одного заказа в DDS, затем итог; позиции здесь не присоединяем.
select 'delivered_all'::text as summary_key,
    count(*)::bigint as orders_count,
    coalesce(sum(o.items_price), 0)::numeric(18,2) as revenue_amount,
    coalesce(sum(o.items_freight), 0)::numeric(18,2) as freight_amount,
    coalesce(sum(o.items_value), 0)::numeric(18,2) as order_value_amount,
    coalesce(sum(o.items_count), 0)::bigint as line_count,
    coalesce(sum(o.items_count), 0)::bigint as unit_count
from {{ ref('fct_order__active') }} o
join {{ ref('dim_order_status') }} s on s.order_status = o.order_status and s.is_current and s.is_revenue
