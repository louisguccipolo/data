-- Покупатель = customer_unique_id, повторный = >=2 заказов любого статуса всей выгрузки.
-- 96 096 покупателей; 2 997 повторных; 93 099 с одним заказом; 99 441 заказ.
-- repeat_share = 0.031187562… (3.12% при показе), это не когортный retention.
select summary_key, customers_count, repeat_customers_count, one_order_customers_count, orders_count,
    repeat_customers_count::numeric / nullif(customers_count, 0) as repeat_share
from mart.customer_summary;
