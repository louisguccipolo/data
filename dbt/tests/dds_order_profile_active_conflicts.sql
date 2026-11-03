with latest as ({{ dds_latest('eff_order_profile', 'order_profile_hk') }})
select order_hk from latest where not is_deleted group by 1 having count(*) > 1
