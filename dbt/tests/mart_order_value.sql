{{ config(severity='error') }}
select * from {{ ref('order_summary') }}
where order_value_amount is distinct from revenue_amount + freight_amount
