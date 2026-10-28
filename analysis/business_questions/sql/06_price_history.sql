-- Только позиции активных доходных заказов; товар постоянный (product_hk), категория не дробит историю.
-- Лидер: 437c05a395e9e47f9762e677a7068ce7 — 31 различная цена, 151 единица.
-- Ранжирование: число цен DESC, единицы DESC, product_id ASC; витрина хранит все товары.
select distinct product_id, distinct_price_count, product_unit_count
from mart.product_price_history
order by distinct_price_count desc, product_unit_count desc, product_id
limit 1;

with leader as (
    select product_id from mart.product_price_history
    order by distinct_price_count desc, product_unit_count desc, product_id limit 1
)
select sale_date, price, unit_count
from mart.product_price_history
where product_id = (select product_id from leader)
order by sale_date nulls last, price;
