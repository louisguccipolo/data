{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ hub(['stg_order_reviews'], 'review_hk', 'review_id', ghost=false) }}
