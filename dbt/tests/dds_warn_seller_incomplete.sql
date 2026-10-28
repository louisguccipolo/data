{{ config(severity='warn') }}
select seller_sk from {{ ref('dim_seller') }} where is_incomplete
