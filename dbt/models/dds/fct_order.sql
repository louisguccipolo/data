{{ config(materialized='table', schema='dds') }}

with orders as (
 select distinct on (order_id) order_id, customer_id, order_status, order_purchase_timestamp
 from {{ ref('stg_orders') }}
 order by order_id, load_dts desc, batch_id desc, order_hashdiff desc, order_status_hashdiff desc
), amounts as (
 select order_id, sum(price)::numeric(18,2) as revenue,
 sum(freight_value)::numeric(18,2) as freight_value, count(*) as line_count,
 sum(unit_count) as unit_count
 from {{ ref('fct_order_item') }} group by order_id
)
select o.*, c.customer_hk,
 coalesce(a.revenue,0)::numeric(18,2) as revenue,
 coalesce(a.freight_value,0)::numeric(18,2) as freight_value,
 coalesce(a.revenue+a.freight_value,0)::numeric(18,2) as order_total,
 coalesce(a.line_count,0)::bigint as line_count, coalesce(a.unit_count,0)::bigint as unit_count
from orders o left join amounts a using(order_id)
left join {{ ref('dim_customer_profile') }} c using(customer_id)
