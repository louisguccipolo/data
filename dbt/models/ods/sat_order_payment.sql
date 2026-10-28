{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ multi_active_sat('stg_order_payments') }}
