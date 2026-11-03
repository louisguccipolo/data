{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ sat('stg_order_reviews', 'review_hk', ['review_score', 'review_comment_title', 'review_comment_message', 'review_creation_date', 'review_answer_timestamp'], 'review_hashdiff') }}
