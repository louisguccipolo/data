{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ rts('stg_order_items__keys', 'order_item_hk') }}
