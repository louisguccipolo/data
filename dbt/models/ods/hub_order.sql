{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ hub(['stg_orders', 'stg_order_items', 'stg_order_payments', 'stg_order_reviews'], 'order_hk', 'order_id', ghost=false) }}
