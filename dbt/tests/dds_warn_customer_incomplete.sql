{{ config(severity='warn') }}
select customer_sk from {{ ref('dim_customer') }} where is_incomplete
