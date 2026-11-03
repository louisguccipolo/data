#!/usr/bin/env bash
# Генерирует .env со случайными паролями (только [A-Za-z0-9] — без экранирования в DSN и XML).
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] && { echo ".env уже есть"; exit 0; }
gen() { LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c "${1:-24}"; }
{
  echo "AIRFLOW_UID=$(id -u)"
  for v in DWH_PASSWORD CH_READER_PASSWORD DWH_ANALYST_PASSWORD \
           CLICKHOUSE_PASSWORD CLICKHOUSE_BI_PASSWORD AIRFLOW_DB_PASSWORD; do
    echo "$v=$(gen)"
  done
  echo "AIRFLOW_JWT_SECRET=$(gen 40)"
} > .env
chmod 600 .env
echo ".env создан"
