{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ link('stg_orders', 'order_profile_hk', ['order_hk', 'customer_profile_hk']) }}
