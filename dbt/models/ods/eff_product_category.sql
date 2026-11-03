{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ relationship_effectivity('stg_products', 'product_hk', 'product_category_hk', 'category_hk') }}
