#!/bin/bash
# Read-only пользователь для BI. Видит только витрины mart.*
set -e
clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery <<SQL
CREATE USER IF NOT EXISTS bi_reader IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_BI_PASSWORD}'
    SETTINGS readonly = 1;
GRANT SELECT ON mart.* TO bi_reader;
SQL
