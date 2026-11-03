-- depends_on: {{ ref('err_cast') }}
with categories as (
    -- Код категории принадлежит версии товара на момент продажи (§20 DDS).
    select p.category_code as category_key,
        count(distinct i.order_id)::bigint as orders_count,
        count(*)::bigint as unit_count,
        coalesce(sum(i.price), 0)::numeric(18,2) as revenue_amount
    from {{ ref('fct_order_item__active') }} i
    join {{ ref('fct_order__active') }} o on o.order_hk = i.order_hk
    join {{ ref('dim_order_status') }} s on s.order_status = o.order_status and s.is_current and s.is_revenue
    join {{ ref('dim_product') }} p on p.product_sk = i.product_sk
    group by p.category_code
), names as (
    -- Предпочитаем текущие товары; при их отсутствии — последнюю версию кода.
    -- Равные даты разрешаем постоянным HK и SK для детерминизма.
    select distinct on (category_code) category_code, category_name_pt, category_name_en, is_translation_missing
    from {{ ref('dim_product') }}
    order by category_code, is_current desc, valid_from desc, product_hk, product_sk
)
select c.category_key, n.category_name_pt as category_name_original,
    n.category_name_en as category_name_english,
    case when c.category_key = '__unknown__' then 'Категория не указана'
        else coalesce(n.category_name_en, n.category_name_pt) end::text as category_label,
    (c.category_key = '__unknown__')::boolean as is_unknown_category,
    n.is_translation_missing::boolean as is_translation_missing,
    c.orders_count, c.unit_count, c.revenue_amount,
    coalesce(c.revenue_amount / nullif(sum(c.revenue_amount) over (), 0), 0)::double precision as revenue_share
from categories c join names n on n.category_code = c.category_key
