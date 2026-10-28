-- depends_on: {{ ref('err_cast') }}
with points as (
select seller_hk, load_dts as valid_from from {{ ref('hub_seller') }}
union
select seller_hk, load_dts as valid_from from {{ ref('sat_seller') }}
union
select seller_hk, load_dts as valid_from from {{ ref('eff_seller') }}
), p as (
    select p.* from points p join {{ ref('hub_seller') }} h using (seller_hk)
    where p.valid_from <= (select as_of_dts from {{ ref('int_as_of') }}) and p.valid_from >= h.load_dts and seller_hk <> repeat('0',32)
), s as (
    select distinct on (p.seller_hk, p.valid_from) p.valid_from, p.seller_hk as point_hk, t.*
    from p left join {{ ref('sat_seller') }} t on t.seller_hk = p.seller_hk and t.load_dts <= p.valid_from
    order by p.seller_hk, p.valid_from, t.load_dts desc
), e as (
    select distinct on (p.seller_hk, p.valid_from) p.valid_from, p.seller_hk as point_hk, t.*
    from p left join {{ ref('eff_seller') }} t on t.seller_hk = p.seller_hk and t.load_dts <= p.valid_from
    order by p.seller_hk, p.valid_from, t.load_dts desc
)
select p.seller_hk, p.valid_from, h.seller_id,
    s.seller_zip_code_prefix, s.seller_city, s.seller_state, g.region as seller_region,
    coalesce(e.is_deleted, false) as is_deleted, s.seller_hk is null as is_incomplete
from p join {{ ref('hub_seller') }} h using (seller_hk)
left join s on s.point_hk = p.seller_hk and s.valid_from = p.valid_from
left join e on e.point_hk = p.seller_hk and e.valid_from = p.valid_from
left join {{ ref('seed_br_state') }} g on g.state_code = upper(trim(s.seller_state))
