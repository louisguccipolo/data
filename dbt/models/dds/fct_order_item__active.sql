-- depends_on: {{ ref('err_cast') }}
{{ config(materialized='view') }}
select * from {{ ref('fct_order_item') }} where not is_deleted
