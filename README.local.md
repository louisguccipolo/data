# Локальный запуск

Ветка dev. `bash scripts/gen_env.sh`, затем `bash scripts/download_data.sh`.

```sh
docker compose -f compose.yaml -f compose.superset.yaml up -d --build
docker compose run --rm dwh-init
docker compose exec airflow python /opt/project/scripts/load_raw.py
docker compose exec airflow python -m pipelines.source_to_raw
docker compose exec airflow bash -c 'cd /opt/project/dbt && dbt build'
```

Superset: http://localhost:18088, пользователь admin, пароль SUPERSET_ADMIN_PASSWORD в .env.
Подключение Marketplace DWH создаётся автоматически. Доступ на чтение к ods, dds, mart и ml. После первого dbt build выполните:

```sh
docker compose -f compose.yaml -f compose.superset.yaml exec superset python /app/config/bootstrap.py
```

В Superset появятся шесть датасетов ODS.
Airflow: http://localhost:18090. PostgreSQL источник: localhost:15432; DWH: localhost:15433.
ClickHouse HTTP: localhost:18123. Пароли находятся в .env (не добавлять в Git).

В этой версии проекта DDS и mart пока не реализованы, список MARTS для ClickHouse пуст.

## Дашборд

[Скриншот дашборда пяти бизнес-вопросов](docs/dashboards/README.md).
SQL и определения метрик: [analysis/business_questions/sql](analysis/business_questions/sql/README.md).
