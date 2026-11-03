{{ config(materialized='view', schema='ods') }}
{# EL snapshots the dependent BK. Resolve the latest SAME-source observation as
   of each snapshot; changing product/seller retires the previous link. The
   equijoin permits a hash join rather than a scan per snapshot key. #}
with snapshots as (
    select distinct {{ hash_key(['order_id', 'order_item_id']) }} as driving_hk,
        _batch_id as batch_id, _extracted_at as load_dts
    from {{ source('raw', 'order_items__keys') }}
), observations as (
    select {{ hash_key(['order_id', '_raw_order_item_id']) }} as driving_hk,
        order_item_hk, load_dts
    from {{ ref('stg_order_items') }}
)
select distinct on (k.driving_hk, k.batch_id, k.load_dts)
    o.order_item_hk, k.batch_id, k.load_dts,
    'order_items__keys'::text as record_source
from snapshots k
left join observations o on o.driving_hk = k.driving_hk and o.load_dts <= k.load_dts
order by k.driving_hk, k.batch_id, k.load_dts, o.load_dts desc, o.order_item_hk
