select
    product_id, seller_id, category_id,
    name as product_name, coalesce(brand, 'n/a') as brand,
    price::numeric(12, 2) as current_price, is_active, created_at, updated_at
from ({{ latest_version('products', 'product_id') }}) s
