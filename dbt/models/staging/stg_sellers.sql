{{ config(materialized='view', schema='ods') }}
with typed as (
select
    nullif(trim(seller_id), '') as seller_id,
    nullif(trim(seller_zip_code_prefix), '') as seller_zip_code_prefix,
    nullif(trim(seller_city), '') as seller_city,
    nullif(trim(seller_state), '') as seller_state,
    seller_id as _raw_seller_id,
    _extracted_at as load_dts,
    _source_file as record_source,
    _batch_id as batch_id
from {{ source('raw', 'sellers') }}
)
select typed.*,
    {{ hash_key(['seller_id']) }} as seller_hk,
    {{ hashdiff(['seller_zip_code_prefix', 'seller_city', 'seller_state']) }} as seller_hashdiff
from typed
