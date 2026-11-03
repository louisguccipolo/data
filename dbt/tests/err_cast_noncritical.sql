{{ config(severity='warn') }}
select * from {{ ref('err_cast') }} where severity = 'warn'
