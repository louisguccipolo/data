-- Сначала суммы одного доставленного заказа, затем итог: средние из числителей и знаменателя.
-- Чек включает доставку: 15 424 463.65 / 96 478 = 159.8754498… BRL (159.88 при показе).
-- Товары: 13 221 498.11; доставка: 2 202 965.54; строки = единицы = 110 197.
select summary_key, orders_count, revenue_amount, freight_amount, order_value_amount,
    line_count, unit_count,
    order_value_amount / nullif(orders_count, 0) as avg_order_value,
    line_count::numeric / nullif(orders_count, 0) as avg_lines,
    unit_count::numeric / nullif(orders_count, 0) as avg_units
from mart.order_summary;
