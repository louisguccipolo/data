-- depends_on: {{ ref('err_cast') }}
with statuses as (
    select order_status, count(*)::bigint as orders_count,
        count(*) filter (where items_count = 0)::bigint as orders_without_items_count
    from {{ ref('fct_order__active') }} group by order_status
)
select s.order_status::text as order_status, s.status_group::text as status_group,
    coalesce(o.orders_count, 0)::bigint as orders_count,
    (coalesce(o.orders_count, 0)::numeric / nullif(sum(o.orders_count) over (), 0))::double precision as order_share,
    coalesce(o.orders_without_items_count, 0)::bigint as orders_without_items_count
from {{ ref('dim_order_status') }} s left join statuses o using (order_status)
where s.is_current
