select customer_profile_hk, load_dts from {{ ref('stg_customers') }} group by 1, 2 having count(distinct customer_profile_lhk) > 1
