{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ link('stg_order_reviews', 'order_review_hk', ['order_hk', 'review_hk']) }}
