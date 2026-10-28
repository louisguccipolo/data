SHELL := /bin/bash
DC := docker compose
-include .env
export

.PHONY: env up down nuke ps data seed el run dbt psql-source psql-dwh psql-analyst ch logs

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

dbt:            ## make dbt ARGS="build -s +sales_wide"
	$(DC) exec airflow bash -c "cd /opt/project/dbt && dbt $(ARGS)"

psql-source:    ## источник (raw организаторов)
	$(DC) exec source psql -U hack -d marketplace

psql-dwh:       ## владелец DWH
	$(DC) run --rm --entrypoint psql dwh-init

psql-analyst:   ## под ролью аналитика — для демо разграничения PII
	$(DC) run --rm -e PGUSER=analyst_user -e PGPASSWORD=$(DWH_ANALYST_PASSWORD) --entrypoint psql dwh-init

ch:
	$(DC) exec clickhouse clickhouse-client --user etl --password $(CLICKHOUSE_PASSWORD) -d mp

logs:
	$(DC) logs -f --tail 100 $(S)

.PHONY: ml-marts
ml-marts:       ## собрать Parquet для ML-ноутбуков без DWH
	.venv/bin/python marts/build.py
