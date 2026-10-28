{{ config(materialized='table', schema='dds') }}
select distinct on (customer_id) customer_id, customer_hk, customer_city, customer_state
from {{ ref('stg_customers') }} s
where customer_id is not null
order by customer_id, load_dts desc, batch_id desc, to_jsonb(s)::text desc
