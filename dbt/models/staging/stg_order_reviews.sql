{{ config(materialized='view', schema='ods') }}
with typed as (
select
    nullif(trim(review_id), '') as review_id,
    nullif(trim(order_id), '') as order_id,
    {{ safe_cast('review_score', 'integer') }} as review_score,
    nullif(trim(review_comment_title), '') as review_comment_title,
    nullif(trim(review_comment_message), '') as review_comment_message,
    {{ safe_cast('review_creation_date', 'timestamp') }} as review_creation_date,
    {{ safe_cast('review_answer_timestamp', 'timestamp') }} as review_answer_timestamp,
    review_id as _raw_review_id,
    order_id as _raw_order_id,
    review_score as _raw_review_score,
    review_creation_date as _raw_review_creation_date,
    review_answer_timestamp as _raw_review_answer_timestamp,
    _extracted_at as load_dts,
    _source_file as record_source,
    _batch_id as batch_id
from {{ source('raw', 'order_reviews') }}
)
select typed.*,
    {{ hash_key(['order_id']) }} as order_hk,
    {{ hash_key(['review_id']) }} as review_hk,
    {{ hash_key(['order_id', 'review_id']) }} as order_review_hk,
    {{ hashdiff(['review_score', 'review_comment_title', 'review_comment_message', 'review_creation_date', 'review_answer_timestamp']) }} as review_hashdiff
from typed
