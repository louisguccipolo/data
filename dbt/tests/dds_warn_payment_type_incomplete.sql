{{ config(severity='warn') }}
select payment_type_sk from {{ ref('dim_payment_type') }} where is_incomplete
