{{ config(severity='warn') }}
select order_status_sk from {{ ref('dim_order_status') }} where is_incomplete
