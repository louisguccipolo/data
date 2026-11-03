# ТЗ: загрузка CSV Olist в `source` (`make seed`)

Контекст: `.plans/marketplace-dwh-spec.md`, §4.1 и §20 («Скрипт загрузки CSV Olist в `source` + `make seed`»).
Образец — `scripts/load_raw.py` организаторов (github.com/daniilivanovp-droid/hackathon-students). Результат в `source` должен совпадать с тем, что даёт их скрипт, байт в байт. Тогда dbt-модели переносимы на их базу.

## Что есть сейчас

- Сервис `source` (Postgres 16, `hack/hack`, БД `marketplace`, хост-порт `127.0.0.1:15432`). Init-скрипты `source/init/01_init.sql`, `source/init/02_raw_ddl.sql` (копия организаторов) уже создали схемы `raw/core/mart` и 8 пустых таблиц `raw.*`: все колонки `text`, плюс `_source_file text` и `_loaded_at timestamptz DEFAULT now()`.
- Контейнер `airflow`: Python 3.11, psycopg2 2.9, репозиторий смонтирован в `/opt/project`, есть `SOURCE_DSN=host=source port=5432 dbname=marketplace user=hack password=hack`.
- Цель `make seed` сейчас вызывает устаревший `oltp/seed.py init` (старая архитектура).

## Что сделать

### 1. `scripts/download_data.sh`
- Скачивает 8 CSV из `https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/<file>` в `data/` в корне репозитория (путь считается от расположения скрипта, а не от cwd).
- Файлы: `olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_order_reviews_dataset.csv`, `olist_products_dataset.csv`, `olist_sellers_dataset.csv`, `olist_customers_dataset.csv`, `product_category_name_translation.csv`.
- Идемпотентно: непустой файл не перекачивается. Скачивание идёт во временный файл и переименовывается после успеха, чтобы оборванная закачка не оставила полфайла. `set -euo pipefail`, `curl -fsSL --retry 3`.
- `olist_geolocation_dataset.csv` НЕ качаем: организаторы его не выдавали, это кандидат на вброс.

### 2. `scripts/load_raw.py`
CLI:
```
python scripts/load_raw.py [--dsn DSN] [--data-dir DIR] [--recreate] [--dry-run]
```
- `--dsn`: по умолчанию env `SOURCE_DSN`, иначе `host=localhost port=15432 dbname=marketplace user=hack password=hack`.
- `--data-dir`: по умолчанию env `DATA_DIR`, иначе `<repo>/data` (от расположения скрипта).
- MAPPING (файл → таблица → колонки) как у организаторов: те же 8 пар и тот же порядок колонок. Таблица данных, а не код: добавить файл = добавить строку.

Поведение:
1. **Предпроверки до подключения к БД:** все 8 файлов на месте (если нет, ошибка со списком недостающих и подсказкой `make data`). Заголовок каждого CSV (чтение в `utf-8-sig`) совпадает с ожидаемым списком колонок. Если нет, ошибка с «в файле / ожидаем».
2. **Подсчёт записей независимо от COPY:** число записей в каждом файле через `csv.reader` (без заголовка; в `order_reviews` есть многострочные комментарии, поэтому `wc -l` не годится).
3. `--dry-run`: только шаги 1–2, печать таблицы «файл → записей», в БД не ходим, exit 0.
4. **DDL:** `CREATE SCHEMA IF NOT EXISTS raw/core/mart`. Файл `source/init/02_raw_ddl.sql` выполняется, только если хотя бы одной из 8 таблиц нет или передан `--recreate`. По умолчанию таблицы НЕ пересоздаются: при вбросе в них могут появиться новые колонки, их нельзя терять.
5. **Загрузка, всё в ОДНОЙ транзакции** (атомарно: при любой ошибке rollback, `raw` остаётся как был). Для каждой таблицы:
   - `TRUNCATE raw.<t>`;
   - `COPY raw.<t> (<колонки>) FROM STDIN WITH (FORMAT csv, HEADER true)`, файл открыт в `utf-8-sig`, `newline=""`. Отдаём файловый объект потоком, без чтения целиком в память. Семантика пустых значений как у организаторов: пустое поле без кавычек → NULL, `""` → пустая строка. CSV НЕ перепарсивать в Python перед COPY, иначе эта разница потеряется;
   - `UPDATE raw.<t> SET _source_file = %s`, имя файла без пути (как у организаторов);
   - **самопроверка:** `count(*)` в таблице = числу записей из шага 2. Если не равно, исключение и rollback всей загрузки, в сообщении таблица и обе цифры.
6. После commit: `ANALYZE` по 8 таблицам.
7. Печать: таблица → строк, итого, время. Exit 0 при успехе, ненулевой код и понятное сообщение при ошибке (нет файлов, кривой заголовок, нет подключения, расхождение счётчиков).
- Имена таблиц и колонок в SQL собирать через `psycopg2.sql.Identifier`, а не f-строками.
- Зависимости: только stdlib + psycopg2.
- Docstring модуля на русском, в стиле остального репо (см. `pipelines/oltp_to_raw.py`): что делает, как запускать, почему TRUNCATE+COPY, и что это значит для EL (у всех строк перезалитой таблицы новый `_loaded_at`, инкремент заберёт её целиком; модель это переживает за счёт hashdiff, см. спеку §4.2).

### 3. `Makefile`
- Новая цель `data` (скачать CSV): `./scripts/download_data.sh`.
- `seed` заменить: зависит от `data`, запускает `$(DC) exec airflow python /opt/project/scripts/load_raw.py`. Комментарий `##`: «залить CSV Olist в source.raw (идемпотентно, TRUNCATE+COPY)».
- Обновить `.PHONY`. Цель `tick` и прочие не трогать.

### 4. `.gitignore`
- Добавить `data/`.

## Не трогать
`oltp/`, `pipelines/`, `airflow/dags/`, `dbt/`, `compose.yaml`, `source/init/*`, `dwh/`. Ничего не коммитить.

## Критерии приёмки (проверяет Claude на живом стенде)
1. `make data` скачивает 8 файлов в `data/`; повторный запуск ничего не качает; md5 совпадают с эталонной локальной копией.
2. `make seed` на пустой `source` проходит. Строк: orders 99 441, order_items 112 650, order_payments 103 886, customers 99 441, products 32 951, sellers 3 095, product_category_translation 71, order_reviews = числу записей CSV.
3. Повторный `make seed` даёт те же счётчики, без дублей.
4. Содержимое совпадает с результатом `load_raw.py` организаторов, загруженным в отдельную БД: `EXCEPT` в обе стороны по всем колонкам, кроме `_loaded_at`, даёт 0 строк, в том числе по NULL и `''`.
5. `_source_file` и `_loaded_at` заполнены у всех строк.
6. Атомарность: при битом заголовке или отсутствующем файле ненулевой код, данные в `raw` не изменились.
7. `--dry-run` работает без БД. `data/` в `.gitignore`.

---

# ТЗ v2: только версия Olist с GitHub, один в один

## Зачем
Копия Olist с Kaggle отличается от GitHub-версии, которую качают организаторы:
- zip-индексы на GitHub без ведущего нуля;
- у 67 позиций `freight_value` = `price`;
- у 3 оплат `payment_installments` = 10 вместо 1.

Сейчас `download_data.sh` пропускает любой непустой файл в `data/`, а `load_raw.py` грузит что угодно с правильным заголовком. Если подложить в `data/` файлы с Kaggle, они тихо уедут в `source`, и контрольные цифры разъедутся (средний чек 159.83 вместо 159.88). Нужна жёсткая гарантия: в `source` попадают только байт-в-байт файлы из GitHub Olist.

## Эталон
- Репозиторий `olist/work-at-olist-data`, коммит `d9e49802f3e92d09ee94ab9ccc5e457f207a8959` (текущий `master`, 2022-03-04). Ссылки строятся на коммит, а не на `master`: если upstream изменится, мы этого не почувствуем.
- sha256 файлов (сняты с этого коммита и сверены):
```
c26c17f59f3027a6a0dbeb8f1fa373c38f3323ecc84f225a8f4820e5bb288df8  olist_customers_dataset.csv
4f6abdbbc94036d0df4a76fa0520c072e31a40119d70f7f370fba1e2285d2bcb  olist_order_items_dataset.csv
61674868ed7b872aac3dcd95bf774448a88f6426b183f21be55f75af021ffdec  olist_order_payments_dataset.csv
012b61c7593e34f51fa614efdf802b9c7056ce6aae5307ddb93236e7cfc797d7  olist_order_reviews_dataset.csv
8df58ef3d2d7e9944010f7beecd9b75367f5588ec6e3c91cec19ae3345ef9ecf  olist_orders_dataset.csv
3e6569628a17fbc75fd206ee357b59e20364b9afa90f5b6cd5b4d624c58aa9cc  olist_products_dataset.csv
31eb9bd50d684526a78f2389413ce61841e97692de7b828fe7d956b534586312  olist_sellers_dataset.csv
a81f0d1f27b27e7293f761bc79e3ce8f348ee39c4b3ed3e49bde38f478586278  product_category_name_translation.csv
```

## Что сделать

### 1. `scripts/olist.sha256` (новый, в git)
Ровно 8 строк выше, формат `sha256sum` (`<hash>  <file>`). Единственный источник правды для обоих скриптов. Первой строкой комментарий `#` с репозиторием и коммитом. Строки с `#` оба скрипта пропускают; если `sha256sum -c` спотыкается о комментарий, комментарий не нужен, коммит тогда указать в `download_data.sh`.

### 2. `scripts/download_data.sh`
- `base_url` = `https://raw.githubusercontent.com/olist/work-at-olist-data/<commit>/datasets`. Коммит вынести в переменную `OLIST_COMMIT`.
- Список файлов брать из `olist.sha256`, а не дублировать массивом.
- Для каждого файла:
  - есть и sha256 совпал: «уже скачан, sha256 ок», пропустить;
  - есть, но sha256 другой: «не та версия (не GitHub Olist), перекачиваю», скачать заново и заменить;
  - нет: скачать.
- Скачанный файл перед `mv` проверяется по sha256. Если не совпал, временный файл удалить, ошибка, exit ≠ 0. Атомарный `mv` из временного файла оставить.
- `sha256sum` (coreutils). Если его нет, fallback на `shasum -a 256`, чтобы работало на macOS.

### 3. `scripts/load_raw.py`
- Проверка sha256 обязательна и идёт первой, до проверки заголовков, и в том числе при `--dry-run`. Флага для её отключения НЕТ.
- Манифест читается из `scripts/olist.sha256`, путь от расположения скрипта. MAPPING остаётся как есть. Набор файлов в манифесте и в MAPPING должен совпадать, иначе ошибка (защита от рассинхрона при вбросе).
- Хеш считается потоково через `hashlib.sha256`, блоками, без чтения файла целиком.
- При несовпадении ошибка: список файлов с ожидаемым и фактическим хешем, текст «в data/ не GitHub-версия Olist (вероятно, Kaggle). Выполните make data». Exit 1, в БД не ходим.
- Docstring: одна-две строки о том, почему проверяем хеши (Kaggle ≠ GitHub, контрольные цифры).
- Вывод `--dry-run` и обычного запуска: добавить строку «sha256: 8/8 совпадают с olist/work-at-olist-data@<короткий коммит>». Коммит взять из комментария манифеста; если его там нет, просто «8/8 совпадают с манифестом».

### 4. Ничего больше не трогать
Makefile (`seed` уже зависит от `data`), `.gitignore`, остальное не менять. Не коммитить.

## Критерии приёмки
1. Чистый `data/`: `make data` скачивает 8 файлов, sha256 сходятся.
2. Повторный `make data` ничего не качает.
3. В `data/` подложены файлы с Kaggle: `make data` перекачивает ровно 4 отличающихся (customers, order_items, order_payments, sellers), после этого все 8 совпадают.
4. Файлы с Kaggle подложены, `load_raw.py` вызван напрямую (мимо `make data`): exit 1, ошибка называет 4 файла, `raw` в `source` не изменилась.
5. `--dry-run` на файлах с Kaggle падает, на GitHub-файлах проходит и печатает строку про sha256.
6. `make seed` на GitHub-данных: результат тот же, что раньше (550 759 строк, совпадение с эталоном организаторов).
