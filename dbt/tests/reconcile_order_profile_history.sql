with observations as (
    select distinct order_hk, order_profile_hk, load_dts from {{ ref('stg_orders') }}
), ordered as (
    select *, lag(order_profile_hk) over (partition by order_hk order by load_dts) as previous from observations
), expected as (
    select order_profile_hk, order_hk, false as is_deleted, load_dts from ordered where order_profile_hk is distinct from previous
    union all
    select previous, order_hk, true, load_dts from ordered where previous is not null and order_profile_hk is distinct from previous
), actual as (
    select order_profile_hk, order_hk, is_deleted, load_dts from {{ ref('eff_order_profile') }}
)
(select * from expected except select * from actual)
union all
(select * from actual except select * from expected)
