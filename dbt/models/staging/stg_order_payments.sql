{{ config(materialized='view', schema='ods') }}
with typed as (
select
    nullif(trim(order_id), '') as order_id,
    {{ safe_cast('payment_sequential', 'integer') }} as payment_sequential,
    nullif(trim(payment_type), '') as payment_type,
    {{ safe_cast('payment_installments', 'integer') }} as payment_installments,
    {{ safe_cast('payment_value', 'numeric(18,2)') }} as payment_value,
    order_id as _raw_order_id,
    payment_sequential as _raw_payment_sequential,
    payment_installments as _raw_payment_installments,
    payment_value as _raw_payment_value,
    _extracted_at as load_dts,
    _source_file as record_source,
    _batch_id as batch_id
from {{ source('raw', 'order_payments') }}
)
select typed.*,
    {{ hash_key(['order_id']) }} as order_hk,
    {{ hashdiff(['payment_sequential', 'payment_type', 'payment_installments', 'payment_value']) }} as payment_row_hashdiff
from typed
