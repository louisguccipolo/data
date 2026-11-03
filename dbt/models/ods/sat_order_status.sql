{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ sat('stg_orders', 'order_hk', ['order_status', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date'], 'order_status_hashdiff') }}
