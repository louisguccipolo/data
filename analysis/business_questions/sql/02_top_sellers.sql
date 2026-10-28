-- Собственные позиции продавца в доставленных заказах, без доставки.
-- Лидер: 4869f7a5dfa277a7dca6462dcf3b52b2, 226 987.93 BRL.
-- В витрине все продавцы; orders_count между продавцами не складывается.
select seller_id, seller_city, seller_state, orders_count, line_count, unit_count, revenue_amount
from mart.seller_sales
order by revenue_amount desc, seller_id
limit 10;
