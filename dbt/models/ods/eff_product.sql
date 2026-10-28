{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ eff_sat('rts_product', 'product_hk') }}
