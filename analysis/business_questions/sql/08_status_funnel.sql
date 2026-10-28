-- Все члены dim_order_status, включая нулевые статусы и __unknown__.
-- Доля 0–1 от всех 99 441 активных заказов; 775 заказов без позиций не теряются.
-- delivered: 96 478; shipped: 1 107; canceled: 625; unavailable: 609.
select order_status, status_group, orders_count,
    round(order_share::numeric, 8) as order_share, orders_without_items_count
from mart.order_status_funnel
order by orders_count desc, order_status;
