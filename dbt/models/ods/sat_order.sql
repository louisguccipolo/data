{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ sat('stg_orders', 'order_hk', ['order_purchase_timestamp', 'order_estimated_delivery_date'], 'order_hashdiff') }}
