#!/bin/bash
# Read-only пользователь для BI (Metabase). Видит только витрины mp.*
set -e
clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery <<SQL
CREATE USER IF NOT EXISTS bi IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_BI_PASSWORD}'
    SETTINGS readonly = 1;
GRANT SELECT ON mp.* TO bi;
SQL
