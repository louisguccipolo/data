with latest as ({{ dds_latest('eff_customer_profile', 'customer_profile_lhk') }})
select customer_profile_hk from latest where not is_deleted group by 1 having count(*) > 1
