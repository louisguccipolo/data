{{ config(materialized='table', schema='dds') }}

with latest as (
 select distinct on (order_id, order_item_id) order_id, order_item_id, product_id, seller_id, price, freight_value
 from {{ ref('stg_order_items') }}
 order by order_id, order_item_id, load_dts desc, batch_id desc, order_item_hk desc
)
select *, 1::bigint as unit_count from latest
