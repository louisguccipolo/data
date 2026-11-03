select order_hk, load_dts from {{ ref('stg_orders') }} group by 1, 2 having count(distinct order_profile_hk) > 1
