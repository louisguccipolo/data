{{ config(materialized='view', schema='ods') }}
select {{ hash_key(['seller_id']) }} as seller_hk, _batch_id as batch_id,
_extracted_at as load_dts, 'sellers__keys'::text as record_source
from {{ source('raw', 'sellers__keys') }}
