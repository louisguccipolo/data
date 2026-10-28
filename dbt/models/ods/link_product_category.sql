{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ link('stg_products', 'product_category_hk', ['product_hk', 'category_hk']) }}
