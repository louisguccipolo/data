{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ sat('stg_customers', 'customer_profile_hk', ['customer_zip_code_prefix', 'customer_city', 'customer_state'], 'customer_profile_hashdiff') }}
