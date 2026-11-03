-- depends_on: {{ ref('err_cast') }}
with sellers as (
    -- Постоянный HK объединяет продажи всех версий продавца.
    select i.seller_hk, count(distinct i.order_id)::bigint as orders_count,
        count(*)::bigint as line_count, count(*)::bigint as unit_count,
        coalesce(sum(i.price), 0)::numeric(18,2) as revenue_amount
    from {{ ref('fct_order_item__active') }} i
    join {{ ref('fct_order__active') }} o on o.order_hk = i.order_hk
    join {{ ref('dim_order_status') }} t on t.order_status = o.order_status and t.is_current and t.is_revenue
    group by i.seller_hk
)
select coalesce(s.seller_id, '__unknown__')::text as seller_id,
    s.seller_city, s.seller_state, i.orders_count, i.line_count, i.unit_count, i.revenue_amount
from sellers i
-- Текущий адрес по HK; удаление продавца не удаляет его продажи.
left join {{ ref('dim_seller') }} s on s.seller_hk = i.seller_hk and s.is_current
