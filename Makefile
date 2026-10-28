SHELL := /bin/bash
DC := docker compose
-include .env
export

.PHONY: env up down nuke ps data seed el run dbt publish ch-reset-mart psql-source psql-dwh psql-analyst ch logs

env:            ## сгенерировать .env со случайными паролями (если его нет)
	@test -f .env || ./scripts/gen_env.sh

up:             ## поднять всё и проинициализировать DWH
	$(DC) up -d --build
	$(DC) run --rm dwh-init

down:
	$(DC) down

nuke:           ## снести всё вместе с данными
	$(DC) down -v

ps:
	$(DC) ps

data:           ## скачать CSV Olist
	./scripts/download_data.sh

seed: data      ## залить CSV Olist в source.raw (идемпотентно, TRUNCATE+COPY)
	$(DC) exec airflow python /opt/project/scripts/load_raw.py

el:             ## один батч EL вручную
	$(DC) exec airflow python -m pipelines.source_to_raw

run:            ## запустить ELT целиком
	$(DC) exec airflow airflow dags unpause marketplace_elt
	$(DC) exec airflow airflow dags trigger marketplace_elt

dbt:            ## make dbt ARGS="build" (только полный build, §15 спеки)
	$(DC) exec airflow bash -c "cd /opt/project/dbt && dbt $(ARGS)"

psql-source:    ## источник (raw организаторов)
	$(DC) exec source psql -U hack -d marketplace

psql-dwh:       ## владелец DWH
	$(DC) run --rm --entrypoint psql dwh-init

psql-analyst:   ## под ролью аналитика — для демо разграничения PII
	$(DC) run --rm -e PGUSER=analyst_user -e PGPASSWORD=$(DWH_ANALYST_PASSWORD) --entrypoint psql dwh-init

ch:
	$(DC) exec clickhouse clickhouse-client --user etl --password $(CLICKHOUSE_PASSWORD) -d mart

publish:        ## опубликовать проверенные витрины DWH mart в ClickHouse
	$(DC) exec -T airflow python -c "from pipelines.mart_to_clickhouse import publish; print(publish('manual'))"

ch-reset-mart:  ## разовая миграция шлифовки блока Б; после dbt build, без активных DAG/publish
	$(DC) exec -T clickhouse bash -euc 'for table in product_price_history payments_by_type payments_by_state_year delivery_by_state review_delivery; do for suffix in "" __stg; do clickhouse-client --user "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD" --query "DROP TABLE IF EXISTS mart.$${table}$${suffix} SYNC"; done; done'
	$(MAKE) publish

logs:
	$(DC) logs -f --tail 100 $(S)

.PHONY: ml-marts
ml-marts:       ## собрать Parquet для ML-ноутбуков без DWH
	.venv/bin/python marts/build.py
