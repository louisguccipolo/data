{{ config(materialized='table', schema='dds') }}
select d::date as date_day, date_trunc('month',d)::date as month_start from generate_series(date '2016-09-01',date '2018-08-31',interval '1 day') d
