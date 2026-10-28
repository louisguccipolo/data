-- Q4. Покупатели и повторные покупки. Покупатель = hub_customer (customer_unique_id).
-- customer_id — профиль покупателя в конкретном заказе (hub_customer_profile), он новый на каждый заказ: 99 441.
-- Вернулся = больше одного заказа в любом статусе.
with per_customer as (
    select lcp.customer_hk, count(distinct lop.order_hk) as orders
    from ods.link_order_profile lop
    join ods.link_customer_profile lcp using (customer_profile_hk)
    group by 1
)
select count(*)                                                        as customers,
       count(*) filter (where orders > 1)                              as returned,
       round(100.0 * count(*) filter (where orders > 1) / count(*), 2) as returned_pct
from per_customer;
