select order_item_hk, load_dts
from {{ ref('stg_order_items') }}
group by order_item_hk, load_dts having count(distinct order_item_hashdiff) > 1
