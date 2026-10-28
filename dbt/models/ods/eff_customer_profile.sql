{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ relationship_effectivity('stg_customers', 'customer_profile_hk', 'customer_profile_lhk', 'customer_hk') }}
