-- depends_on: {{ ref('err_cast') }}
with points as (
select product_hk, load_dts as valid_from from {{ ref('hub_product') }}
union
select product_hk, load_dts as valid_from from {{ ref('sat_product') }}
union
select product_hk, load_dts as valid_from from {{ ref('eff_product') }}
union
select product_hk, load_dts from {{ ref('eff_product_category') }}
union
select l.product_hk, s.load_dts
from {{ ref('link_product_category') }} l join {{ ref('sat_category') }} s
    on s.category_hk = l.category_hk and s.load_dts >= l.load_dts
union
select l.product_hk, greatest(l.load_dts, h.load_dts) from {{ ref('link_product_category') }} l join {{ ref('hub_category') }} h using (category_hk)
), p as (
    select p.* from points p join {{ ref('hub_product') }} h using (product_hk)
    where p.valid_from <= (select as_of_dts from {{ ref('int_as_of') }}) and p.valid_from >= h.load_dts and product_hk <> repeat('0',32)
), s as (
    select distinct on (p.product_hk, p.valid_from) p.valid_from, p.product_hk as point_hk, t.*
    from p left join {{ ref('sat_product') }} t on t.product_hk = p.product_hk and t.load_dts <= p.valid_from
    order by p.product_hk, p.valid_from, t.load_dts desc
), e as (
    select distinct on (p.product_hk, p.valid_from) p.valid_from, p.product_hk as point_hk, t.*
    from p left join {{ ref('eff_product') }} t on t.product_hk = p.product_hk and t.load_dts <= p.valid_from
    order by p.product_hk, p.valid_from, t.load_dts desc
), relationships as (
    select distinct on (p.product_hk, p.valid_from, t.product_category_hk)
        p.product_hk, p.valid_from, t.category_hk, t.is_deleted
    from p join {{ ref('eff_product_category') }} t
      on t.product_hk = p.product_hk and t.load_dts <= p.valid_from
    order by p.product_hk, p.valid_from, t.product_category_hk, t.load_dts desc
), category as (
    select distinct on (p.product_hk, p.valid_from) p.product_hk, p.valid_from,
        h.product_category_name, t.product_category_name_english
    from p left join relationships r on r.product_hk = p.product_hk
        and r.valid_from = p.valid_from and not r.is_deleted
    left join {{ ref('hub_category') }} h on h.category_hk = r.category_hk
        and h.category_hk <> repeat('0',32) and h.load_dts <= p.valid_from
    left join {{ ref('sat_category') }} t on t.category_hk = h.category_hk and t.load_dts <= p.valid_from
    order by p.product_hk, p.valid_from, t.load_dts desc
)
select p.product_hk, p.valid_from, h.product_id,
    s.product_name_lenght as product_name_length, s.product_description_lenght as product_description_length,
    s.product_photos_qty, s.product_weight_g, s.product_length_cm, s.product_height_cm, s.product_width_cm,
    coalesce(c.product_category_name, '__unknown__') as category_code,
    c.product_category_name as category_name_pt, c.product_category_name_english as category_name_en,
    coalesce(c.product_category_name_english, c.product_category_name, '__unknown__') as category_display_name,
    c.product_category_name is not null and c.product_category_name_english is null as is_translation_missing,
    coalesce(e.is_deleted, false) as is_deleted, s.product_hk is null as is_incomplete
from p join {{ ref('hub_product') }} h using (product_hk)
left join s on s.point_hk = p.product_hk and s.valid_from = p.valid_from
left join e on e.point_hk = p.product_hk and e.valid_from = p.valid_from
left join category c on c.product_hk = p.product_hk and c.valid_from = p.valid_from
