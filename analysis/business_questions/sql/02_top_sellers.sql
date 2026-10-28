select seller_id, city, state, orders_count, unit_count, revenue from mart.seller_sales order by revenue desc, seller_id limit 10;
