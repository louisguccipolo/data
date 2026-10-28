{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ hub(['stg_sellers', 'stg_order_items'], 'seller_hk', 'seller_id', ghost=false) }}
