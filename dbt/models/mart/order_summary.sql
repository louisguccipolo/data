{{ config(materialized='table', schema='mart') }}

select 'delivered_all'::text as scope, count(*) as orders_count,
 sum(revenue)::numeric(18,2) as revenue, sum(order_total)::numeric(18,2) as total_with_freight,
 sum(line_count)::bigint as line_count, sum(unit_count)::bigint as unit_count,
 round(avg(order_total),2) as average_order_with_freight,
 avg(line_count) as average_lines, avg(unit_count) as average_units
from {{ ref('fct_order') }} where order_status='delivered'
