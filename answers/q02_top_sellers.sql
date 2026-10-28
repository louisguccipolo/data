-- Q2. Топ-10 продавцов по выручке. Уровень — строка заказа (link_order_item):
-- продавцу идёт только price его позиций, а не сумма всего заказа (1 278 заказов с несколькими продавцами).
-- Единица товара = строка order_items: колонки количества в источнике нет.
with status as (
    select distinct on (order_hk) order_hk, order_status
    from ods.sat_order_status order by order_hk, load_dts desc
), items as (
    select distinct on (order_item_hk) order_item_hk, price
    from ods.sat_order_item order by order_item_hk, load_dts desc
), sellers as (
    select distinct on (seller_hk) seller_hk, seller_city, seller_state
    from ods.sat_seller order by seller_hk, load_dts desc
)
select h.seller_id,
       s.seller_city,
       s.seller_state,
       count(distinct l.order_hk) as orders,
       count(*)                   as units,
       sum(i.price)               as revenue
from ods.link_order_item l
join items i using (order_item_hk)
join status st using (order_hk)
join ods.hub_seller h using (seller_hk)
left join sellers s using (seller_hk)
where st.order_status = 'delivered'
group by 1, 2, 3
order by revenue desc
limit 10;
