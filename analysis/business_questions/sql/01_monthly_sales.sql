-- 24 месяца покупки [2016-09, 2018-09), пропуски заполнены нулями.
-- Выручка без доставки: 13 221 498.11 BRL; доставленных заказов: 96 478.
-- Ноябрь 2016: все три показателя равны нулю.
select month, orders_all_count, orders_delivered_count, revenue_amount
from mart.monthly_sales
order by month;
