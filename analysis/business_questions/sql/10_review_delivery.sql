-- Один последний активный отзыв на заказ: creation DESC, answer DESC, review_id DESC.
-- NULL-даты уступают известным; NULL-оценка сохраняется отдельной группой.
-- Все статусы в orders_count; доставка — is_revenue с датой, без соединения с позициями.
-- 98 673 заказа с отзывом; оценка 1: 21.3247 дня и доля опозданий 0.366941;
-- оценка 5: 10.6810 дня и 0.018611. Это описательная связь, не причинный эффект.
select review_score, orders_count, delivered_measured_count,
    round(avg_delivery_days, 4) as avg_delivery_days, late_count,
    round(late_share::numeric, 6) as late_share
from mart.review_delivery
order by review_score nulls last;
