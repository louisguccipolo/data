{{ config(materialized='table', schema='mart') }}

with months as (select distinct month_start from {{ ref('dim_date') }}),
sales as (
 select date_trunc('month',order_purchase_timestamp)::date as month_start,
 count(*) as orders_all_count, count(*) filter(where order_status='delivered') as orders_count,
 sum(revenue) filter(where order_status='delivered') as revenue
 from {{ ref('fct_order') }} group by 1
)
select m.month_start, coalesce(s.orders_all_count,0)::bigint as orders_all_count,
 coalesce(s.orders_count,0)::bigint as orders_count, coalesce(s.revenue,0)::numeric(18,2) as revenue
from months m left join sales s using(month_start)
