{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ hub(['stg_products', 'stg_product_category_translation'], 'category_hk', 'product_category_name', ghost=true) }}
