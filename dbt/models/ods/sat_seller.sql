{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ sat('stg_sellers', 'seller_hk', ['seller_zip_code_prefix', 'seller_city', 'seller_state'], 'seller_hashdiff') }}
