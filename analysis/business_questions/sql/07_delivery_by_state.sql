-- Штат из заказа на момент привязки (§20), пустой — __unknown__; название/регион из dim_geo.
-- Порог >= 500 — все активные заказы любого статуса; на Olist 17 штатов.
-- Срок и опоздания — только is_revenue с датой доставки, из to_customer_days и is_late DDS.
-- SP: 41 746 заказов, 40 494 измеренных, 8.7614 дня, 1 820 опозданий (доля 0.044945).
select customer_state, state_name, region, orders_count, delivered_measured_count,
    round(avg_delivery_days, 4) as avg_delivery_days, late_count,
    round(late_share::numeric, 6) as late_share
from mart.delivery_by_state
where orders_count >= 500
order by avg_delivery_days, customer_state;
