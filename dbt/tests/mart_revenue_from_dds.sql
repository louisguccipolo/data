{{ config(severity='error') }}
with expected as (
    select coalesce(sum(o.items_price), 0) as revenue_amount
    from {{ ref('fct_order__active') }} o
    join {{ ref('dim_order_status') }} s on s.order_status = o.order_status and s.is_current and s.is_revenue
)
select m.revenue_amount as actual, e.revenue_amount as expected
from {{ ref('order_summary') }} m cross join expected e
where m.revenue_amount is distinct from e.revenue_amount
