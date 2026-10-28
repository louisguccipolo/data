{{ config(materialized='table', schema='dds') }}
select distinct on (seller_id) seller_id, seller_city, seller_state
from {{ ref('stg_sellers') }} s
where seller_id is not null
order by seller_id, load_dts desc, batch_id desc, to_jsonb(s)::text desc
