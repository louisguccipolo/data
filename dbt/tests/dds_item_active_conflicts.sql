select order_hk, order_item_id from {{ ref('int_order_item_candidates') }} where priority = 1 group by 1, 2 having count(*) > 1
