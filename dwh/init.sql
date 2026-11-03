-- Postgres DWH: слои и роли.
-- Идемпотентно: docker compose run --rm dwh-init
--
--   raw   — копия источника как пришло (все колонки text). Пишет только ETL.
--   ods   — Data Vault: hub_* / link_* / sat_*.
--   dds   — звезда: dim_* / fct_*.
--   mart  — витрины и ответы на вопросы, источник для ClickHouse и reverse ETL.
--   ml    — предсказания моделей (пишет ML).

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS ml;

-- ---------- Роли ----------
-- analyst   : ods, dds, mart, ml (raw — только ETL)
-- ch_reader : только mart — ClickHouse ходит сюда за витринами
SELECT 'CREATE ROLE analyst NOLOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst') \gexec
SELECT 'CREATE ROLE ch_reader LOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ch_reader') \gexec
SELECT 'CREATE ROLE analyst_user LOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_user') \gexec

ALTER ROLE ch_reader    PASSWORD :'ch_reader_password';
ALTER ROLE analyst_user PASSWORD :'analyst_password';

GRANT analyst TO analyst_user;

REVOKE ALL ON SCHEMA raw FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

GRANT USAGE ON SCHEMA ods, dds, mart, ml TO analyst;
GRANT USAGE ON SCHEMA mart TO ch_reader;

-- dbt пересоздаёт таблицы на каждом прогоне — права выдаём через default privileges,
-- тогда новые таблицы сразу получают нужные гранты.
GRANT SELECT ON ALL TABLES IN SCHEMA ods, dds, mart, ml TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO ch_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE dwh IN SCHEMA ods  GRANT SELECT ON TABLES TO analyst;
ALTER DEFAULT PRIVILEGES FOR ROLE dwh IN SCHEMA dds  GRANT SELECT ON TABLES TO analyst;
ALTER DEFAULT PRIVILEGES FOR ROLE dwh IN SCHEMA mart GRANT SELECT ON TABLES TO analyst, ch_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE dwh IN SCHEMA ml   GRANT SELECT ON TABLES TO analyst;
