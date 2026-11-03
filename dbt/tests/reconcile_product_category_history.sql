with observations as (
    select distinct product_hk, product_category_hk, load_dts
    from {{ ref('stg_products') }}
), ordered as (
    select *, lag(product_category_hk) over (partition by product_hk order by load_dts) as previous
    from observations
), expected as (
    select product_category_hk, product_hk, false as is_deleted, load_dts
    from ordered where product_category_hk is distinct from previous
    union all
    select previous, product_hk, true, load_dts
    from ordered where previous is not null and product_category_hk is distinct from previous
), actual as (
    select product_category_hk, product_hk, is_deleted, load_dts
    from {{ ref('eff_product_category') }}
)
(select * from expected except select * from actual)
union all
(select * from actual except select * from expected)
