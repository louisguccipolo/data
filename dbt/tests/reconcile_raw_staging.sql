select 'orders' as relation_name where (select count(*) from {{ source('raw', 'orders') }}) <> (select count(*) from {{ ref('stg_orders') }})
union all
select 'order_items' as relation_name where (select count(*) from {{ source('raw', 'order_items') }}) <> (select count(*) from {{ ref('stg_order_items') }})
union all
select 'order_payments' as relation_name where (select count(*) from {{ source('raw', 'order_payments') }}) <> (select count(*) from {{ ref('stg_order_payments') }})
union all
select 'order_reviews' as relation_name where (select count(*) from {{ source('raw', 'order_reviews') }}) <> (select count(*) from {{ ref('stg_order_reviews') }})
union all
select 'products' as relation_name where (select count(*) from {{ source('raw', 'products') }}) <> (select count(*) from {{ ref('stg_products') }})
union all
select 'sellers' as relation_name where (select count(*) from {{ source('raw', 'sellers') }}) <> (select count(*) from {{ ref('stg_sellers') }})
union all
select 'customers' as relation_name where (select count(*) from {{ source('raw', 'customers') }}) <> (select count(*) from {{ ref('stg_customers') }})
union all
select 'product_category_translation' as relation_name where (select count(*) from {{ source('raw', 'product_category_translation') }}) <> (select count(*) from {{ ref('stg_product_category_translation') }})
