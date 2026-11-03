{{ config(severity='warn') }}
with leader as (
    select seller_id, revenue_amount from {{ ref('seller_sales') }}
    order by revenue_amount desc, seller_id limit 1
)
select 'seller_leader' as check_name
where not exists (select 1 from leader
    where seller_id = '4869f7a5dfa277a7dca6462dcf3b52b2' and revenue_amount = 226987.93)
