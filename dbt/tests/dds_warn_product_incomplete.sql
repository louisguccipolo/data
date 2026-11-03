{{ config(severity='warn') }}
select product_sk from {{ ref('dim_product') }} where is_incomplete
