{{ config(materialized='view', schema='ods') }}
with typed as (
select
    nullif(trim(order_id), '') as order_id,
    {{ safe_cast('order_item_id', 'integer') }} as order_item_id,
    nullif(trim(product_id), '') as product_id,
    nullif(trim(seller_id), '') as seller_id,
    {{ safe_cast('shipping_limit_date', 'timestamp') }} as shipping_limit_date,
    {{ safe_cast('price', 'numeric(18,2)') }} as price,
    {{ safe_cast('freight_value', 'numeric(18,2)') }} as freight_value,
    order_id as _raw_order_id,
    order_item_id as _raw_order_item_id,
    product_id as _raw_product_id,
    seller_id as _raw_seller_id,
    shipping_limit_date as _raw_shipping_limit_date,
    price as _raw_price,
    freight_value as _raw_freight_value,
    _extracted_at as load_dts,
    _source_file as record_source,
    _batch_id as batch_id
from {{ source('raw', 'order_items') }}
)
select typed.*,
    {{ hash_key(['order_id']) }} as order_hk,
    {{ hash_key(['product_id']) }} as product_hk,
    {{ hash_key(['seller_id']) }} as seller_hk,
    {{ hash_key(['order_id', 'product_id', 'seller_id', '_raw_order_item_id']) }} as order_item_hk,
    {{ hashdiff(['price', 'freight_value', 'shipping_limit_date']) }} as order_item_hashdiff
from typed
