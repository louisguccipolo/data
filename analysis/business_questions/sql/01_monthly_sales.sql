select to_char(month_start,'YYYY-MM') as month, orders_count, revenue from mart.monthly_sales order by month_start;
