{{ config(materialized='view', schema='ods') }}
with typed as (
select
    nullif(trim(customer_id), '') as customer_id,
    nullif(trim(customer_unique_id), '') as customer_unique_id,
    nullif(trim(customer_zip_code_prefix), '') as customer_zip_code_prefix,
    nullif(trim(customer_city), '') as customer_city,
    nullif(trim(customer_state), '') as customer_state,
    customer_id as _raw_customer_id,
    customer_unique_id as _raw_customer_unique_id,
    _extracted_at as load_dts,
    _source_file as record_source,
    _batch_id as batch_id
from {{ source('raw', 'customers') }}
)
select typed.*,
    {{ hash_key(['customer_id']) }} as customer_profile_hk,
    {{ hash_key(['customer_unique_id']) }} as customer_hk,
    {{ hash_key(['customer_id', 'customer_unique_id']) }} as customer_profile_lhk,
    {{ hashdiff(['customer_zip_code_prefix', 'customer_city', 'customer_state']) }} as customer_profile_hashdiff
from typed
