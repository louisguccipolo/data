{{ config(severity='warn') }}
select geo_sk from {{ ref('dim_geo') }} where is_incomplete
