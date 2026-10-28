{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ sat('stg_product_category_translation', 'category_hk', ['product_category_name_english'], 'category_hashdiff') }}
