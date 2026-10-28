{{ config(materialized='table', schema='mart') }}

with customers as (
 select customer_hk, count(*) as orders_count from {{ ref('fct_order') }}
 where customer_hk is not null group by 1
)
select 'all_customers'::text as scope, count(*) as customers_count,
 count(*) filter(where orders_count>=2) as repeat_customers_count,
 round(100.0 * count(*) filter(where orders_count>=2)/nullif(count(*),0),4) as repeat_pct
from customers
