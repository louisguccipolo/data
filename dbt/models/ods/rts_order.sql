{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ rts('stg_orders__keys', 'order_hk') }}
