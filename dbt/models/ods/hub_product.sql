{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ hub(['stg_products', 'stg_order_items'], 'product_hk', 'product_id', ghost=false) }}
