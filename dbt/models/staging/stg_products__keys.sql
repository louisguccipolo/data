{{ config(materialized='view', schema='ods') }}
select {{ hash_key(['product_id']) }} as product_hk, _batch_id as batch_id,
_extracted_at as load_dts, 'products__keys'::text as record_source
from {{ source('raw', 'products__keys') }}
