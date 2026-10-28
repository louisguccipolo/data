{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ sat('stg_order_items', 'order_item_hk', ['price', 'freight_value', 'shipping_limit_date'], 'order_item_hashdiff') }}
