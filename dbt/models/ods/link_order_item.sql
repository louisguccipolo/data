{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ link('stg_order_items', 'order_item_hk', ['order_hk', 'product_hk', 'seller_hk', 'order_item_id']) }}
