with observations as (
    select distinct customer_profile_hk, customer_profile_lhk, load_dts from {{ ref('stg_customers') }}
), ordered as (
    select *, lag(customer_profile_lhk) over (partition by customer_profile_hk order by load_dts) as previous from observations
), expected as (
    select customer_profile_lhk, customer_profile_hk, false as is_deleted, load_dts from ordered where customer_profile_lhk is distinct from previous
    union all
    select previous, customer_profile_hk, true, load_dts from ordered where previous is not null and customer_profile_lhk is distinct from previous
), actual as (
    select customer_profile_lhk, customer_profile_hk, is_deleted, load_dts from {{ ref('eff_customer_profile') }}
)
(select * from expected except select * from actual)
union all
(select * from actual except select * from expected)
