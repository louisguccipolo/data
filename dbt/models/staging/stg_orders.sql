select
    order_id, user_id as customer_id, warehouse_id, delivery_city_id,
    status as order_status, coalesce(payment_method, 'unknown') as payment_method,
    created_at, paid_at, shipped_at, delivered_at, cancelled_at, returned_at, updated_at
from ({{ latest_version('orders', 'order_id') }}) s
