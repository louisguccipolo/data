{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ rts('stg_order_reviews__keys', 'review_hk') }}
