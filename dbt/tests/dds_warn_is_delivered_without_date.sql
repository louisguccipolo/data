{{ config(severity='warn') }}
select order_hk from {{ ref('fct_order') }} where is_delivered_without_date
