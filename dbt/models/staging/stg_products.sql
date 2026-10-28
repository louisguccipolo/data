{{ config(materialized='view', schema='ods') }}
with typed as (
select
    nullif(trim(product_id), '') as product_id,
    nullif(trim(product_category_name), '') as product_category_name,
    {{ safe_cast('product_name_lenght', 'integer') }} as product_name_lenght,
    {{ safe_cast('product_description_lenght', 'integer') }} as product_description_lenght,
    {{ safe_cast('product_photos_qty', 'integer') }} as product_photos_qty,
    {{ safe_cast('product_weight_g', 'integer') }} as product_weight_g,
    {{ safe_cast('product_length_cm', 'integer') }} as product_length_cm,
    {{ safe_cast('product_height_cm', 'integer') }} as product_height_cm,
    {{ safe_cast('product_width_cm', 'integer') }} as product_width_cm,
    product_id as _raw_product_id,
    product_category_name as _raw_product_category_name,
    product_name_lenght as _raw_product_name_lenght,
    product_description_lenght as _raw_product_description_lenght,
    product_photos_qty as _raw_product_photos_qty,
    product_weight_g as _raw_product_weight_g,
    product_length_cm as _raw_product_length_cm,
    product_height_cm as _raw_product_height_cm,
    product_width_cm as _raw_product_width_cm,
    _extracted_at as load_dts,
    _source_file as record_source,
    _batch_id as batch_id
from {{ source('raw', 'products') }}
)
select typed.*,
    {{ hash_key(['product_id']) }} as product_hk,
    {{ hash_key(['product_category_name']) }} as category_hk,
    {{ hash_key(['product_id', 'product_category_name']) }} as product_category_hk,
    {{ hashdiff(['product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']) }} as product_hashdiff
from typed
