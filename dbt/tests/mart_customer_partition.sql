{{ config(severity='error') }}
select * from {{ ref('customer_summary') }}
where customers_count is distinct from repeat_customers_count + one_order_customers_count
