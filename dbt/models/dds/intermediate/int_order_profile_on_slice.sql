-- depends_on: {{ ref('err_cast') }}
with op as ({{ dds_latest('eff_order_profile', 'order_profile_hk') }}),
cp as ({{ dds_latest('eff_customer_profile', 'customer_profile_lhk') }}),
bindings as (
    select op.order_hk, op.customer_profile_hk, cp.customer_hk, l.load_dts as bind_dts
    from op join {{ ref('link_order_profile') }} l using (order_profile_hk)
    left join cp on cp.customer_profile_hk = op.customer_profile_hk and not cp.is_deleted
    where not op.is_deleted
), profile as (
    -- Prefer the last observation at binding; otherwise the first on slice.
    select distinct on (b.order_hk) b.order_hk,
        p.customer_zip_code_prefix, p.customer_city, p.customer_state
    from bindings b join {{ ref('sat_customer_profile') }} p
        on p.customer_profile_hk = b.customer_profile_hk
        and p.load_dts <= (select as_of_dts from {{ ref('int_as_of') }})
    order by b.order_hk, (p.load_dts <= b.bind_dts) desc,
        case when p.load_dts <= b.bind_dts then p.load_dts end desc,
        p.load_dts
)
select b.order_hk, b.customer_profile_hk, b.customer_hk, h.customer_id,
    p.customer_zip_code_prefix, p.customer_city, p.customer_state, b.bind_dts
from bindings b left join profile p using (order_hk)
left join {{ ref('hub_customer_profile') }} h on h.customer_profile_hk = b.customer_profile_hk
