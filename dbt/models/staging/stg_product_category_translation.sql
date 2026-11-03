{{ config(materialized='view', schema='ods') }}
with typed as (
select
    nullif(trim(product_category_name), '') as product_category_name,
    nullif(trim(product_category_name_english), '') as product_category_name_english,
    product_category_name as _raw_product_category_name,
    _extracted_at as load_dts,
    _source_file as record_source,
    _batch_id as batch_id
from {{ source('raw', 'product_category_translation') }}
)
select typed.*,
    {{ hash_key(['product_category_name']) }} as category_hk,
    {{ hashdiff(['product_category_name_english']) }} as category_hashdiff
from typed
