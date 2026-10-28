-- depends_on: {{ ref('err_cast') }}
{{ config(materialized='view') }}
select * from {{ ref('fct_payment') }} where not is_deleted
