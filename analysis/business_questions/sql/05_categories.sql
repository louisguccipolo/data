-- 74 исходные категории, включая __unknown__; выручка доставленных позиций без доставки.
-- Лидер: beleza_saude / health_beauty = 1 233 131.72 BRL; итог = 13 221 498.11 BRL.
-- revenue_share — доля 0–1 от всех категорий; топ-10 не меняет знаменатель.
select category_key, category_name_original, category_name_english, category_label,
    is_unknown_category, is_translation_missing, orders_count, unit_count, revenue_amount, revenue_share
from mart.category_sales
order by revenue_amount desc, category_key;
