with latest as ({{ dds_latest('eff_product_category', 'product_category_hk') }})
select product_hk from latest where not is_deleted group by 1 having count(*) > 1
