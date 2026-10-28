-- depends_on: {{ ref('err_cast') }}
with op as ({{ dds_latest('eff_order_profile', 'order_profile_hk') }}),
cp as ({{ dds_latest('eff_customer_profile', 'customer_profile_lhk') }}),
profile as ({{ dds_latest('sat_customer_profile', 'customer_profile_hk') }})
select op.order_hk, op.customer_profile_hk, cp.customer_hk, h.customer_id,
    p.customer_zip_code_prefix, p.customer_city, p.customer_state
from op left join cp on cp.customer_profile_hk = op.customer_profile_hk and not cp.is_deleted
left join profile p on p.customer_profile_hk = op.customer_profile_hk
left join {{ ref('hub_customer_profile') }} h on h.customer_profile_hk = op.customer_profile_hk
where not op.is_deleted
