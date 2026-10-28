{{ config(materialized='table', schema='mart') }}

with sales as (
 select coalesce(p.category,'__unknown__') as category,
 count(distinct i.order_id) as orders_count, count(*) as line_count,
 sum(i.unit_count)::bigint as unit_count, sum(i.price)::numeric(18,2) as revenue
 from {{ ref('fct_order_item') }} i join {{ ref('fct_order') }} o using(order_id)
 left join {{ ref('dim_product') }} p using(product_id)
 where o.order_status='delivered' group by 1
)
select *, round(100.0*revenue/nullif(sum(revenue) over(),0),4) as revenue_share_pct from sales
