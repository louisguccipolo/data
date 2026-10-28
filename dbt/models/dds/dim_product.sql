{{ config(materialized='table', schema='dds') }}
select distinct on (product_id) product_id, coalesce(product_category_name, '__unknown__') as category
from {{ ref('stg_products') }} s
where product_id is not null
order by product_id, load_dts desc, batch_id desc, to_jsonb(s)::text desc
