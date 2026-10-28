{{ config(severity='error') }}
select 'orders_with_review' as check_name from {{ ref('review_delivery') }}
having coalesce(sum(orders_count), 0) <> (
    select count(*) from {{ ref('fct_order__active') }} o
    where exists (select 1 from {{ ref('fct_review__active') }} r where r.order_hk = o.order_hk)
)
union all
select 'invalid_score' from {{ ref('review_delivery') }} where review_score not between 1 and 5
