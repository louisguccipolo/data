{{ config(severity='warn') }}
select 'orders.order_id' as column_name, order_id as business_key from {{ ref('stg_orders') }} where order_id is not null and order_id !~* '^[0-9a-f]{32}$'
union all
select 'orders.customer_id' as column_name, customer_id as business_key from {{ ref('stg_orders') }} where customer_id is not null and customer_id !~* '^[0-9a-f]{32}$'
union all
select 'order_items.order_id' as column_name, order_id as business_key from {{ ref('stg_order_items') }} where order_id is not null and order_id !~* '^[0-9a-f]{32}$'
union all
select 'order_items.product_id' as column_name, product_id as business_key from {{ ref('stg_order_items') }} where product_id is not null and product_id !~* '^[0-9a-f]{32}$'
union all
select 'order_items.seller_id' as column_name, seller_id as business_key from {{ ref('stg_order_items') }} where seller_id is not null and seller_id !~* '^[0-9a-f]{32}$'
union all
select 'order_payments.order_id' as column_name, order_id as business_key from {{ ref('stg_order_payments') }} where order_id is not null and order_id !~* '^[0-9a-f]{32}$'
union all
select 'order_reviews.review_id' as column_name, review_id as business_key from {{ ref('stg_order_reviews') }} where review_id is not null and review_id !~* '^[0-9a-f]{32}$'
union all
select 'order_reviews.order_id' as column_name, order_id as business_key from {{ ref('stg_order_reviews') }} where order_id is not null and order_id !~* '^[0-9a-f]{32}$'
union all
select 'products.product_id' as column_name, product_id as business_key from {{ ref('stg_products') }} where product_id is not null and product_id !~* '^[0-9a-f]{32}$'
union all
select 'sellers.seller_id' as column_name, seller_id as business_key from {{ ref('stg_sellers') }} where seller_id is not null and seller_id !~* '^[0-9a-f]{32}$'
union all
select 'customers.customer_id' as column_name, customer_id as business_key from {{ ref('stg_customers') }} where customer_id is not null and customer_id !~* '^[0-9a-f]{32}$'
union all
select 'customers.customer_unique_id' as column_name, customer_unique_id as business_key from {{ ref('stg_customers') }} where customer_unique_id is not null and customer_unique_id !~* '^[0-9a-f]{32}$'
