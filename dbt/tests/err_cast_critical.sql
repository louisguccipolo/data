select * from {{ ref('err_cast') }} where severity = 'error'
