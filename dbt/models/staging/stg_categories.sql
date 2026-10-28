select category_id, parent_id as parent_category_id, name as category_name, updated_at
from ({{ latest_version('categories', 'category_id') }}) s
