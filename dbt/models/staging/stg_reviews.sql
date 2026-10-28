select
    review_id, order_item_id, user_id as customer_id, product_id, rating, created_at, updated_at
from ({{ latest_version('reviews', 'review_id') }}) s
