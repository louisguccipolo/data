select {{ hash_key(['order_id', '_raw_order_item_id']) }}, load_dts from {{ ref('stg_order_items') }} group by 1,2 having count(distinct order_item_hk) > 1
