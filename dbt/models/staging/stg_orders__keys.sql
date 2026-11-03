{{ config(materialized='view', schema='ods') }}
select {{ hash_key(['order_id']) }} as order_hk, _batch_id as batch_id,
_extracted_at as load_dts, 'orders__keys'::text as record_source
from {{ source('raw', 'orders__keys') }}
