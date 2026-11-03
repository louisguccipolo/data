{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ rts('stg_products__keys', 'product_hk') }}
