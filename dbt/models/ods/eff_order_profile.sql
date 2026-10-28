{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ relationship_effectivity('stg_orders', 'order_hk', 'order_profile_hk', 'customer_profile_hk') }}
