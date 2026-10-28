-- Invariants for any input snapshot (no fixture-specific constants).
select 'monthly_vs_orders' as failure
where (select sum(revenue) from {{ ref('monthly_sales') }}) <>
 (select coalesce(sum(revenue),0) from {{ ref('fct_order') }} where order_status='delivered'
 and order_purchase_timestamp>=date '2016-09-01' and order_purchase_timestamp<date '2018-09-01')
union all select 'seller_vs_category' where
 (select sum(revenue) from {{ ref('seller_sales') }}) <> (select sum(revenue) from {{ ref('category_sales') }})
union all select 'category_vs_order' where
 (select sum(revenue) from {{ ref('category_sales') }}) <> (select revenue from {{ ref('order_summary') }})
union all select 'calendar' where (select count(*) from {{ ref('monthly_sales') }})<>24
union all select 'order_summary_grain' where (select count(*) from {{ ref('order_summary') }} )<>1
union all select 'customer_summary_grain' where (select count(*) from {{ ref('customer_summary') }} )<>1
union all select 'duplicate_items' where exists (
 select order_id,order_item_id from {{ ref('fct_order_item') }} group by 1,2 having count(*)>1)
union all select 'duplicate_orders' where exists (
 select order_id from {{ ref('fct_order') }} group by 1 having count(*)>1)
