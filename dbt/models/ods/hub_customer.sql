{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ hub(['stg_customers'], 'customer_hk', 'customer_unique_id', ghost=false) }}
