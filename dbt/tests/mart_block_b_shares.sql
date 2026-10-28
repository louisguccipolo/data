{{ config(severity='error') }}
with shares as (
    select 'delivery' as model, late_share as share, late_count as numerator,
        delivered_measured_count as denominator from {{ ref('delivery_by_state') }}
    union all
    select 'review', late_share, late_count, delivered_measured_count from {{ ref('review_delivery') }}
    union all
    select 'funnel', order_share, orders_count, sum(orders_count) over () from {{ ref('order_status_funnel') }}
)
select * from shares
where numerator < 0 or numerator > denominator or share < 0 or share > 1
    or (denominator > 0 and (share is null or
        abs(share - (numerator::numeric / denominator)::double precision) > 1e-12))
    or (denominator = 0 and share is not null)
