{{ config(materialized='table', schema='ods') }}
select 'orders'::text as source_table, concat_ws('||', order_id) as business_key, 'order_id'::text as column_name, _raw_order_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_orders') }} where order_id is null
union all
select 'orders'::text as source_table, concat_ws('||', order_id) as business_key, 'customer_id'::text as column_name, _raw_customer_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_orders') }} where customer_id is null
union all
select 'orders'::text as source_table, concat_ws('||', order_id) as business_key, 'order_status'::text as column_name, _raw_order_status::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_orders') }} where order_status is null
union all
select 'orders'::text as source_table, concat_ws('||', order_id) as business_key, 'order_purchase_timestamp'::text as column_name, _raw_order_purchase_timestamp::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_orders') }} where nullif(trim(_raw_order_purchase_timestamp), '') is not null and order_purchase_timestamp is null
union all
select 'orders'::text as source_table, concat_ws('||', order_id) as business_key, 'order_approved_at'::text as column_name, _raw_order_approved_at::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_orders') }} where nullif(trim(_raw_order_approved_at), '') is not null and order_approved_at is null
union all
select 'orders'::text as source_table, concat_ws('||', order_id) as business_key, 'order_delivered_carrier_date'::text as column_name, _raw_order_delivered_carrier_date::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_orders') }} where nullif(trim(_raw_order_delivered_carrier_date), '') is not null and order_delivered_carrier_date is null
union all
select 'orders'::text as source_table, concat_ws('||', order_id) as business_key, 'order_delivered_customer_date'::text as column_name, _raw_order_delivered_customer_date::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_orders') }} where nullif(trim(_raw_order_delivered_customer_date), '') is not null and order_delivered_customer_date is null
union all
select 'orders'::text as source_table, concat_ws('||', order_id) as business_key, 'order_estimated_delivery_date'::text as column_name, _raw_order_estimated_delivery_date::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_orders') }} where nullif(trim(_raw_order_estimated_delivery_date), '') is not null and order_estimated_delivery_date is null
union all
select 'order_items'::text as source_table, concat_ws('||', order_id, _raw_order_item_id) as business_key, 'order_id'::text as column_name, _raw_order_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_order_items') }} where order_id is null
union all
select 'order_items'::text as source_table, concat_ws('||', order_id, _raw_order_item_id) as business_key, 'order_item_id'::text as column_name, _raw_order_item_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_order_items') }} where (nullif(trim(_raw_order_item_id), '') is not null and order_item_id is null) or nullif(trim(_raw_order_item_id), '') is null
union all
select 'order_items'::text as source_table, concat_ws('||', order_id, _raw_order_item_id) as business_key, 'product_id'::text as column_name, _raw_product_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_order_items') }} where product_id is null
union all
select 'order_items'::text as source_table, concat_ws('||', order_id, _raw_order_item_id) as business_key, 'seller_id'::text as column_name, _raw_seller_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_order_items') }} where seller_id is null
union all
select 'order_items'::text as source_table, concat_ws('||', order_id, _raw_order_item_id) as business_key, 'shipping_limit_date'::text as column_name, _raw_shipping_limit_date::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_order_items') }} where nullif(trim(_raw_shipping_limit_date), '') is not null and shipping_limit_date is null
union all
select 'order_items'::text as source_table, concat_ws('||', order_id, _raw_order_item_id) as business_key, 'price'::text as column_name, _raw_price::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_order_items') }} where nullif(trim(_raw_price), '') is not null and price is null
union all
select 'order_items'::text as source_table, concat_ws('||', order_id, _raw_order_item_id) as business_key, 'freight_value'::text as column_name, _raw_freight_value::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_order_items') }} where nullif(trim(_raw_freight_value), '') is not null and freight_value is null
union all
select 'order_payments'::text as source_table, concat_ws('||', order_id, _raw_payment_sequential) as business_key, 'order_id'::text as column_name, _raw_order_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_order_payments') }} where order_id is null
union all
select 'order_payments'::text as source_table, concat_ws('||', order_id, _raw_payment_sequential) as business_key, 'payment_sequential'::text as column_name, _raw_payment_sequential::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_order_payments') }} where (nullif(trim(_raw_payment_sequential), '') is not null and payment_sequential is null) or nullif(trim(_raw_payment_sequential), '') is null
union all
select 'order_payments'::text as source_table, concat_ws('||', order_id, _raw_payment_sequential) as business_key, 'payment_installments'::text as column_name, _raw_payment_installments::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_order_payments') }} where nullif(trim(_raw_payment_installments), '') is not null and payment_installments is null
union all
select 'order_payments'::text as source_table, concat_ws('||', order_id, _raw_payment_sequential) as business_key, 'payment_value'::text as column_name, _raw_payment_value::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_order_payments') }} where nullif(trim(_raw_payment_value), '') is not null and payment_value is null
union all
select 'order_reviews'::text as source_table, concat_ws('||', review_id, order_id) as business_key, 'review_id'::text as column_name, _raw_review_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_order_reviews') }} where review_id is null
union all
select 'order_reviews'::text as source_table, concat_ws('||', review_id, order_id) as business_key, 'order_id'::text as column_name, _raw_order_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_order_reviews') }} where order_id is null
union all
select 'order_reviews'::text as source_table, concat_ws('||', review_id, order_id) as business_key, 'review_score'::text as column_name, _raw_review_score::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_order_reviews') }} where nullif(trim(_raw_review_score), '') is not null and review_score is null
union all
select 'order_reviews'::text as source_table, concat_ws('||', review_id, order_id) as business_key, 'review_creation_date'::text as column_name, _raw_review_creation_date::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_order_reviews') }} where nullif(trim(_raw_review_creation_date), '') is not null and review_creation_date is null
union all
select 'order_reviews'::text as source_table, concat_ws('||', review_id, order_id) as business_key, 'review_answer_timestamp'::text as column_name, _raw_review_answer_timestamp::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_order_reviews') }} where nullif(trim(_raw_review_answer_timestamp), '') is not null and review_answer_timestamp is null
union all
select 'products'::text as source_table, concat_ws('||', product_id) as business_key, 'product_id'::text as column_name, _raw_product_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_products') }} where product_id is null
union all
select 'products'::text as source_table, concat_ws('||', product_id) as business_key, 'product_name_lenght'::text as column_name, _raw_product_name_lenght::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_products') }} where nullif(trim(_raw_product_name_lenght), '') is not null and product_name_lenght is null
union all
select 'products'::text as source_table, concat_ws('||', product_id) as business_key, 'product_description_lenght'::text as column_name, _raw_product_description_lenght::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_products') }} where nullif(trim(_raw_product_description_lenght), '') is not null and product_description_lenght is null
union all
select 'products'::text as source_table, concat_ws('||', product_id) as business_key, 'product_photos_qty'::text as column_name, _raw_product_photos_qty::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_products') }} where nullif(trim(_raw_product_photos_qty), '') is not null and product_photos_qty is null
union all
select 'products'::text as source_table, concat_ws('||', product_id) as business_key, 'product_weight_g'::text as column_name, _raw_product_weight_g::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_products') }} where nullif(trim(_raw_product_weight_g), '') is not null and product_weight_g is null
union all
select 'products'::text as source_table, concat_ws('||', product_id) as business_key, 'product_length_cm'::text as column_name, _raw_product_length_cm::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_products') }} where nullif(trim(_raw_product_length_cm), '') is not null and product_length_cm is null
union all
select 'products'::text as source_table, concat_ws('||', product_id) as business_key, 'product_height_cm'::text as column_name, _raw_product_height_cm::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_products') }} where nullif(trim(_raw_product_height_cm), '') is not null and product_height_cm is null
union all
select 'products'::text as source_table, concat_ws('||', product_id) as business_key, 'product_width_cm'::text as column_name, _raw_product_width_cm::text as original_value, batch_id, load_dts, 'warn'::text as severity
from {{ ref('stg_products') }} where nullif(trim(_raw_product_width_cm), '') is not null and product_width_cm is null
union all
select 'sellers'::text as source_table, concat_ws('||', seller_id) as business_key, 'seller_id'::text as column_name, _raw_seller_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_sellers') }} where seller_id is null
union all
select 'customers'::text as source_table, concat_ws('||', customer_id) as business_key, 'customer_id'::text as column_name, _raw_customer_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_customers') }} where customer_id is null
union all
select 'customers'::text as source_table, concat_ws('||', customer_id) as business_key, 'customer_unique_id'::text as column_name, _raw_customer_unique_id::text as original_value, batch_id, load_dts, 'error'::text as severity
from {{ ref('stg_customers') }} where customer_unique_id is null
