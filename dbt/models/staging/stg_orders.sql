{{ config(materialized='view', schema='ods') }}
with typed as (
select
    nullif(trim(order_id), '') as order_id,
    nullif(trim(customer_id), '') as customer_id,
    nullif(trim(order_status), '') as order_status,
    {{ safe_cast('order_purchase_timestamp', 'timestamp') }} as order_purchase_timestamp,
    {{ safe_cast('order_approved_at', 'timestamp') }} as order_approved_at,
    {{ safe_cast('order_delivered_carrier_date', 'timestamp') }} as order_delivered_carrier_date,
    {{ safe_cast('order_delivered_customer_date', 'timestamp') }} as order_delivered_customer_date,
    {{ safe_cast('order_estimated_delivery_date', 'timestamp') }} as order_estimated_delivery_date,
    order_id as _raw_order_id,
    customer_id as _raw_customer_id,
    order_status as _raw_order_status,
    order_purchase_timestamp as _raw_order_purchase_timestamp,
    order_approved_at as _raw_order_approved_at,
    order_delivered_carrier_date as _raw_order_delivered_carrier_date,
    order_delivered_customer_date as _raw_order_delivered_customer_date,
    order_estimated_delivery_date as _raw_order_estimated_delivery_date,
    _extracted_at as load_dts,
    _source_file as record_source,
    _batch_id as batch_id
from {{ source('raw', 'orders') }}
)
select typed.*,
    {{ hash_key(['order_id']) }} as order_hk,
    {{ hash_key(['customer_id']) }} as customer_profile_hk,
    {{ hash_key(['order_id', 'customer_id']) }} as order_profile_hk,
    {{ hashdiff(['order_purchase_timestamp', 'order_estimated_delivery_date']) }} as order_hashdiff,
    {{ hashdiff(['order_status', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date']) }} as order_status_hashdiff
from typed
