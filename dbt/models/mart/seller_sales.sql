{{ config(materialized='table', schema='mart') }}

select i.seller_id, s.seller_city as city, s.seller_state as state,
 count(distinct i.order_id) as orders_count, count(*) as line_count,
 sum(i.unit_count)::bigint as unit_count, sum(i.price)::numeric(18,2) as revenue
from {{ ref('fct_order_item') }} i join {{ ref('fct_order') }} o using(order_id)
left join {{ ref('dim_seller') }} s using(seller_id)
where o.order_status='delivered' group by 1,2,3
