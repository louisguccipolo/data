-- depends_on: {{ ref('err_cast') }}
with orders as (
    select coalesce(nullif(upper(trim(o.customer_state)), ''), '__unknown__')::text as customer_state,
        o.to_customer_days, o.is_late,
        coalesce(s.is_revenue, false) and o.order_delivered_customer_ts is not null as measured
    from {{ ref('fct_order__active') }} o
    left join {{ ref('dim_order_status') }} s on s.order_status = o.order_status and s.is_current
), totals as (
    select customer_state, count(*)::bigint as orders_count,
        count(*) filter (where measured)::bigint as delivered_measured_count,
        coalesce(sum(to_customer_days) filter (where measured), 0)::numeric as delivery_days_sum,
        avg(to_customer_days) filter (where measured)::numeric as avg_delivery_days,
        count(*) filter (where measured and is_late)::bigint as late_count
    from orders group by customer_state
)
select t.customer_state, g.state_name::text as state_name, g.region::text as region,
    t.orders_count, t.delivered_measured_count, t.delivery_days_sum, t.avg_delivery_days, t.late_count,
    (t.late_count::numeric / nullif(t.delivered_measured_count, 0))::double precision as late_share
from totals t left join {{ ref('dim_geo') }} g on g.state_code = t.customer_state and g.is_current
