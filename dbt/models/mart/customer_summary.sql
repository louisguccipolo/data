-- depends_on: {{ ref('err_cast') }}
with orders as (
    select o.order_id, c.customer_unique_id
    from {{ ref('fct_order__active') }} o
    left join {{ ref('dim_customer') }} c on c.customer_sk = o.customer_sk
), per_customer as (
    -- По умолчанию Unknown (NULL BK) не считаем одной реальной личностью.
    -- Такие заказы остаются в orders_count, но не в показателях покупателей.
    select customer_unique_id, count(*) as orders_count
    from orders
    where customer_unique_id is not null
    group by 1
)
select 'all_customers'::text as summary_key,
    count(*)::bigint as customers_count,
    count(*) filter (where orders_count >= 2)::bigint as repeat_customers_count,
    count(*) filter (where orders_count = 1)::bigint as one_order_customers_count,
    (select count(*) from orders)::bigint as orders_count
from per_customer
