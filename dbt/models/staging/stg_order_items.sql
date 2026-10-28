select
    order_item_id, order_id, product_id, seller_id,
    quantity, unit_price::numeric(12, 2) as unit_price, discount::numeric(14, 2) as discount_amount,
    created_at, updated_at
from ({{ latest_version('order_items', 'order_item_id') }}) s
