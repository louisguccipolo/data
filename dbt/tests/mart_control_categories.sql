{{ config(severity='warn') }}
with leader as (
    select category_key, category_name_english, revenue_amount from {{ ref('category_sales') }}
    order by revenue_amount desc, category_key limit 1
)
select 'category_count' as check_name from {{ ref('category_sales') }} having count(*) <> 74
union all
select 'category_revenue' from {{ ref('category_sales') }}
having sum(revenue_amount) is distinct from 13221498.11::numeric
union all
select 'category_leader' where not exists (select 1 from leader
    where category_key = 'beleza_saude' and category_name_english = 'health_beauty' and revenue_amount = 1233131.72)
union all
select 'unknown_category' from {{ ref('category_sales') }}
having count(*) filter (where category_key = '__unknown__' and is_unknown_category
    and category_label = 'Категория не указана') <> 1
union all
select 'untranslated_categories' from {{ ref('category_sales') }}
having count(*) filter (where category_key in ('pc_gamer', 'portateis_cozinha_e_preparadores_de_alimentos')
    and is_translation_missing and not is_unknown_category and category_name_english is null
    and category_label = category_name_original) <> 2
