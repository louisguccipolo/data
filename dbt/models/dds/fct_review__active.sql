-- depends_on: {{ ref('err_cast') }}
{{ config(materialized='view') }}
select * from {{ ref('fct_review') }} where not is_deleted
