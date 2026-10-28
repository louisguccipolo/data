-- Q5. Выручка по категориям и доля. Товары без категории ссылаются на нулевой ключ hub_category,
-- поэтому INNER JOIN их не теряет: это 74-я строка «(без категории)». Перевод — LEFT JOIN, у 2 категорий его нет.
-- Контроль: сумма по категориям = итог Q1 (13 221 498.11).
with status as (
    select distinct on (order_hk) order_hk, order_status
    from ods.sat_order_status order by order_hk, load_dts desc
), items as (
    select distinct on (order_item_hk) order_item_hk, price
    from ods.sat_order_item order by order_item_hk, load_dts desc
), translation as (
    select distinct on (category_hk) category_hk, product_category_name_english
    from ods.sat_category order by category_hk, load_dts desc
)
select coalesce(hc.product_category_name, '(без категории)')                       as category,
       coalesce(t.product_category_name_english, hc.product_category_name, 'unknown') as category_en,
       sum(i.price)                                                                  as revenue,
       round(100 * sum(i.price) / sum(sum(i.price)) over (), 2)                      as share_pct
from ods.link_order_item l
join items i using (order_item_hk)
join status s using (order_hk)
join ods.link_product_category lpc using (product_hk)
join ods.hub_category hc using (category_hk)
left join translation t using (category_hk)
where s.order_status = 'delivered'
group by 1, 2
order by revenue desc;
