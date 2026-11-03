{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ hub(['stg_customers', 'stg_orders'], 'customer_profile_hk', 'customer_id', ghost=false) }}
