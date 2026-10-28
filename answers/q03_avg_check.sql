-- Q3. Средний чек. Чек — свойство заказа: сначала сумма price + freight на заказ, потом среднее по заказам.
-- Позиция = заказ × товар × продавец; единица = строка order_items (количества в источнике нет).
with status as (
    select distinct on (order_hk) order_hk, order_status
    from ods.sat_order_status order by order_hk, load_dts desc
), items as (
    select distinct on (order_item_hk) order_item_hk, price, freight_value
    from ods.sat_order_item order by order_item_hk, load_dts desc
), per_order as (
    select l.order_hk,
           sum(i.price + i.freight_value)                       as order_total,
           count(distinct (l.product_hk, l.seller_hk))          as positions,
           count(*)                                             as units
    from ods.link_order_item l
    join items i using (order_item_hk)
    join status s using (order_hk)
    where s.order_status = 'delivered'
    group by 1
)
select round(avg(order_total), 2) as avg_check,
       round(avg(positions), 2)   as avg_positions,
       round(avg(units), 2)       as avg_units,
       count(*)                   as orders
from per_order;
