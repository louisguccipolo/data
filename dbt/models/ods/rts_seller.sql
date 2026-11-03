{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ rts('stg_sellers__keys', 'seller_hk') }}
