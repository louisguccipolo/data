{{ config(severity='warn') }}
select order_hk from {{ ref('fct_order') }} where is_approved_missing
