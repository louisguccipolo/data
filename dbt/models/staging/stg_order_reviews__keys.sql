{{ config(materialized='view', schema='ods') }}
select {{ hash_key(['review_id']) }} as review_hk, _batch_id as batch_id,
_extracted_at as load_dts, 'order_reviews__keys'::text as record_source
from {{ source('raw', 'order_reviews__keys') }}
