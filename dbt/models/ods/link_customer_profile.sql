{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ link('stg_customers', 'customer_profile_lhk', ['customer_profile_hk', 'customer_hk']) }}
