-- Q1. Выручка и число заказов по месяцам, 09.2016–08.2018. Источник — ODS (Data Vault), не raw.
-- Выручка = sum(price) позиций delivered-заказов, без доставки. Месяц — по дате оформления.
-- Календарь строим отдельно и присоединяем LEFT JOIN: ноябрь 2016 без заказов остаётся строкой с нулём.
with status as (
    select distinct on (order_hk) order_hk, order_status
    from ods.sat_order_status order by order_hk, load_dts desc
), orders as (
    select distinct on (order_hk) order_hk, order_purchase_timestamp
    from ods.sat_order order by order_hk, load_dts desc
), items as (
    select distinct on (order_item_hk) order_item_hk, price
    from ods.sat_order_item order by order_item_hk, load_dts desc
), revenue_orders as (
    select o.order_hk,
           date_trunc('month', o.order_purchase_timestamp)::date as month,
           sum(i.price) as revenue
    from orders o
    join status s using (order_hk)
    join ods.link_order_item l using (order_hk)
    join items i using (order_item_hk)
    where s.order_status = 'delivered'
    group by 1, 2
), calendar as (
    select generate_series('2016-09-01'::date, '2018-08-01'::date, interval '1 month')::date as month
)
select c.month,
       count(r.order_hk)              as orders,
       coalesce(sum(r.revenue), 0)    as revenue
from calendar c
left join revenue_orders r using (month)
group by c.month
order by c.month;
