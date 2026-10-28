-- depends_on: {{ ref('err_cast') }}
with latest_review as (
    -- Отсутствующая дата уступает известной; ID разрешает равные даты.
    select distinct on (order_hk) order_hk, review_score
    from {{ ref('fct_review__active') }}
    order by order_hk, review_creation_ts desc nulls last, review_answer_ts desc nulls last, review_id desc
), orders as (
    select r.review_score, o.to_customer_days, o.is_late,
        coalesce(s.is_revenue, false) and o.order_delivered_customer_ts is not null as measured
    from latest_review r join {{ ref('fct_order__active') }} o on o.order_hk = r.order_hk
    left join {{ ref('dim_order_status') }} s on s.order_status = o.order_status and s.is_current
)
select review_score::integer as review_score, count(*)::bigint as orders_count,
    count(*) filter (where measured)::bigint as delivered_measured_count,
    avg(to_customer_days) filter (where measured)::numeric as avg_delivery_days,
    count(*) filter (where measured and is_late)::bigint as late_count,
    (count(*) filter (where measured and is_late)::numeric /
        nullif(count(*) filter (where measured), 0))::double precision as late_share
from orders group by review_score
