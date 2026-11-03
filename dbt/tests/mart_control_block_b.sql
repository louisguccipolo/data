{{ config(severity='warn') }}
with leader as (
    select product_id, distinct_price_count, product_unit_count from {{ ref('product_price_history') }}
    order by distinct_price_count desc, product_unit_count desc, product_id limit 1
)
select 'price_leader' as check_name where not exists (
    select 1 from leader where product_id = '437c05a395e9e47f9762e677a7068ce7'
        and distinct_price_count = 31 and product_unit_count = 151
)
union all
select 'states_500' from {{ ref('delivery_by_state') }} having count(*) filter (where orders_count >= 500) <> 17
union all
select 'funnel' from {{ ref('order_status_funnel') }}
having sum(orders_count) is distinct from 99441::numeric
    or sum(orders_without_items_count) is distinct from 775::numeric

union all
select 'payments' from {{ ref('payments_by_type') }}
having sum(payments_count) is distinct from 103886::numeric
    or sum(payment_amount) is distinct from 16008872.12::numeric
    or sum(installments_sum) is distinct from 296450::numeric
    or sum(zero_installments_count) is distinct from 2::numeric
union all
select 'orders_with_review' from {{ ref('review_delivery') }}
having sum(orders_count) is distinct from 98673::numeric
