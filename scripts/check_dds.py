"""Независимая приёмка dbt DDS и mart на синтетических батчах.

Запуск из корня локального стенда:
    docker compose exec -T airflow python - < scripts/check_dds.py

Все трансформации выполняет dbt. Этот harness создает отдельную временную БД,
копирует в нее только DDL raw, вставляет тестовые строки и проверяет результат.
Живые source/raw/ODS/DDS не изменяются. Нужны права CREATE DATABASE у DWH_DSN;
созданная БД удаляется в finally, а логи остаются в /tmp контейнера Airflow.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

DB = 'dds_review_' + uuid.uuid4().hex[:12]
WORK = Path(tempfile.mkdtemp(prefix='dds-review-'))
PARAMS = psycopg2.extensions.parse_dsn(os.environ['DWH_DSN'])
ADMIN = dict(PARAMS, dbname='postgres')
TEST = dict(PARAMS, dbname=DB)
TABLES = ['orders', 'order_items', 'order_payments', 'order_reviews', 'products',
          'sellers', 'customers', 'product_category_translation', 'orders__keys',
          'order_items__keys', 'order_reviews__keys', 'products__keys', 'sellers__keys']
O, O2, C, C2, U, P, P2, S, S2, R = [c * 32 for c in 'abc123def4']
ROWS = {
    'orders': [dict(order_id=O, customer_id=C, order_status='created',
                    order_purchase_timestamp='2017-01-01 12:00:00', order_estimated_delivery_date='2017-02-01'),
               dict(order_id=O2, customer_id=C2, order_status='delivered',
                    order_purchase_timestamp='2017-01-02 12:00:00', order_estimated_delivery_date='2017-02-01')],
    'order_items': [dict(order_id=O, order_item_id='1', product_id=P, seller_id=S,
                        shipping_limit_date='2017-01-05', price='100.00', freight_value='10.00'),
                    dict(order_id=O2, order_item_id='1', product_id=P2, seller_id=S2,
                        shipping_limit_date='2017-01-05', price='30.00', freight_value='2.00')],
    'order_payments': [dict(order_id=O, payment_sequential='1', payment_type='credit_card', payment_installments='1', payment_value='100.00'),
                       dict(order_id=O, payment_sequential='2', payment_type='voucher', payment_installments='1', payment_value='10.00'),
                       dict(order_id=O2, payment_sequential='1', payment_type='boleto', payment_installments='1', payment_value='32.00')],
    'order_reviews': [dict(review_id=R, order_id=order, review_score='5', review_comment_message='review',
                          review_creation_date='2017-02-01', review_answer_timestamp='2017-02-02') for order in (O, O2)],
    'products': [dict(product_id=P, product_category_name=None, product_weight_g='100'),
                 dict(product_id=P2, product_category_name='cat', product_weight_g='200')],
    'sellers': [dict(seller_id=s, seller_city=' City ', seller_state='SP', seller_zip_code_prefix='01234') for s in (S, S2)],
    'customers': [dict(customer_id=c, customer_unique_id=U, customer_city='Town', customer_state='SP', customer_zip_code_prefix='01234') for c in (C, C2)],
    'product_category_translation': [dict(product_category_name='cat', product_category_name_english='category')],
}
KEYS = {'orders': ['order_id'], 'order_items': ['order_id', 'order_item_id'],
        'order_reviews': ['review_id', 'order_id'], 'products': ['product_id'], 'sellers': ['seller_id']}

def connect():
    return psycopg2.connect(**TEST)

def query(statement, params=()):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(statement, params)
        return cur.fetchall() if cur.description else []

def build(label, full=False, expect_success=True, only=None, as_of=None):
    args = ['dbt', '--no-use-colors', '--log-path', str(WORK / 'logs'), 'build',
            '--project-dir', '/opt/project/dbt', '--profiles-dir', str(WORK),
            '--target-path', str(WORK / 'target'), '--no-partial-parse',
            '--indirect-selection', 'cautious']
    if as_of:
        args.extend(['--vars', json.dumps({'as_of_dts': as_of})])
    if full:
        args.append('--full-refresh')
    # Synthetic dates do not span the fixed 24-month Olist calendar. Only that
    # dataset-specific error test is excluded; all mart invariants and warn controls run.
    args.extend(['--select', *(only or ['+path:models/dds', 'path:models/mart'])])
    args.extend(['--exclude', 'mart_monthly_calendar'])
    result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    (WORK / (label + '.log')).write_text(result.stdout)
    print(label, 'exit=', result.returncode, flush=True)
    summary = [line for line in result.stdout.splitlines() if 'Done.' in line or 'Completed ' in line or 'Failure in' in line or 'Database Error' in line]
    print('\n'.join(summary[-12:]), flush=True)
    assert (result.returncode == 0) == expect_success, str(WORK / (label + '.log')) + '\n' + result.stdout[-6000:]
    return json.loads((WORK / 'target' / 'run_results.json').read_text())

def insert(table, rows, batch):
    if not rows:
        return
    at = f'2026-01-{batch:02d} 00:00:00+00'
    data = []
    for row in rows:
        row = dict(row, _batch_id=f'review-{batch}', _extracted_at=at)
        if not table.endswith('__keys'):
            row.update(_source_file=table + '.csv', _src_loaded_at=at)
        data.append(row)
    columns = sorted(set().union(*(row.keys() for row in data)))
    with connect() as conn, conn.cursor() as cur:
        stmt = sql.SQL('insert into raw.{} ({}) values %s').format(sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier, columns)))
        execute_values(cur, stmt.as_string(cur), [[r.get(c) for c in columns] for r in data])

# Полные наблюдаемые наборы и непустые снимки: ограничения upstream сохранены.
U2, R2, O3, O4, C3, O5, C4 = [c * 32 for c in '567890a']
ROWS['products'] = [dict(product_id=P, product_category_name='cat_a', product_weight_g='100')]
ROWS['product_category_translation'] = [dict(product_category_name='cat_a', product_category_name_english='Category A')]
ROWS['customers'][1].update(customer_city='Other', customer_state='RJ', customer_zip_code_prefix='22222')
ROWS['sellers'][0]['seller_city'] = 'Old'
# A revenue sale before the category/seller change; O5 retains the created status.
ROWS['orders'][0]['order_status'] = 'delivered'
# Two orders already use C2 before its move; O4 first appears in the move batch.
ROWS['orders'].append(dict(ROWS['orders'][1], order_id=O3))
ROWS['orders'].append(dict(ROWS['orders'][0], order_id=O5, customer_id=C4, order_status='created'))
# U2 is known before binding, even though C2 only switches to it in batch 3.
ROWS['customers'].append(dict(ROWS['customers'][0], customer_id=C3, customer_unique_id=U2))

def batch(n):
    import copy
    rows = copy.deepcopy(ROWS)
    if n >= 2:
        rows['products'][0]['product_category_name'] = 'cat_b'
        rows['products'].append(dict(product_id=P2, product_category_name='cat_b', product_weight_g='200'))
        rows['sellers'][0]['seller_city'] = 'New'
        rows['orders'][0]['customer_id'] = C2
        rows['customers'][1].update(customer_city='Moved', customer_state='MG', customer_zip_code_prefix='33333')
        rows['orders'].append(dict(ROWS['orders'][1], order_id=O4))
        rows['order_items'].append(dict(ROWS['order_items'][0], order_id=O4))
        rows['customers'].append(dict(ROWS['customers'][0], customer_id=C4,
                                     customer_unique_id='b' * 32, customer_city='Late'))
        rows['order_payments'] = [ROWS['order_payments'][0], ROWS['order_payments'][2]]
        rows['order_reviews'] = [dict(ROWS['order_reviews'][1], review_id=R2)]
    if n >= 3:
        rows['order_items'][0]['product_id'] = P2
        rows['products'][1]['product_weight_g'] = '300'
        rows['customers'][1]['customer_unique_id'] = U2
        rows['customers'][-1]['customer_city'] = 'Later'
        rows['sellers'] = [rows['sellers'][1]]
        rows['product_category_translation'].append(dict(product_category_name='cat_b', product_category_name_english='Category B'))
    if n == 4:
        rows['products'][0]['product_category_name'] = 'cat_a'
        rows['products'][1]['product_weight_g'] = '400'
        rows['sellers'] = copy.deepcopy(ROWS['sellers'])
    for table, records in rows.items():
        insert(table, records, n)
    for table, keys in KEYS.items():
        insert(table + '__keys', [{k: row.get(k) for k in keys} for row in rows[table]], n)

def fingerprints(include_metadata=False):
    result = {}
    for (table,) in query("select tablename from pg_tables where schemaname='dds' order by tablename"):
        # built_at и invocation_id обязаны меняться; as_of проверяется отдельно.
        projection = 'as_of_dts' if table == 'build_info' and not include_metadata else '*'
        result[table] = query(sql.SQL("select count(*), md5(coalesce(string_agg(md5(t::text), '' order by md5(t::text)), '')) from (select {} from dds.{}) t").format(sql.SQL(projection), sql.Identifier(table)))[0]
    return result

def mart_fingerprints():
    return {table: query(sql.SQL(
        "select count(*), md5(coalesce(string_agg(md5(t::text), '' order by md5(t::text)), '')) from mart.{} t"
    ).format(sql.Identifier(table)))[0] for (table,) in query(
        "select tablename from pg_tables where schemaname='mart' order by tablename")}

def check(condition, message):
    assert condition, message
    print('PASS:', message, flush=True)

admin = psycopg2.connect(**ADMIN)
admin.autocommit = True
try:
    with admin.cursor() as c:
        c.execute(sql.SQL('create database {}').format(sql.Identifier(DB)))
    with psycopg2.connect(**PARAMS) as live, live.cursor() as source, connect() as dest, dest.cursor() as target:
        target.execute('create schema raw')
        for table in TABLES:
            source.execute("select a.attname, format_type(a.atttypid,a.atttypmod) from pg_attribute a join pg_class c on c.oid=a.attrelid join pg_namespace n on n.oid=c.relnamespace where n.nspname='raw' and c.relname=%s and a.attnum>0 and not a.attisdropped order by a.attnum", (table,))
            columns = source.fetchall()
            assert columns, table
            target.execute(sql.SQL('create table raw.{} ({})').format(sql.Identifier(table), sql.SQL(',').join(sql.SQL('{} {}').format(sql.Identifier(name), sql.SQL(typ)) for name, typ in columns)))
    profile = '''marketplace:
  target: review
  outputs:
    review:
      type: postgres
      host: dwh
      port: 5432
      user: dwh
      password: "{{ env_var('DWH_PASSWORD') }}"
      dbname: DBNAME
      schema: ods
      threads: 4
'''
    (WORK / 'profiles.yml').write_text(profile.replace('DBNAME', DB))
    print('Изолированная DDS БД:', DB, 'logs:', WORK, flush=True)
    batch(1)
    build('01_initial')
    initial = fingerprints()
    initial_mart = mart_fingerprints()
    initial_a_revenue = query("select revenue_amount from mart.category_sales where category_key='cat_a'")
    check(query("select p.category_code from dds.fct_order_item i join dds.dim_product p using (product_sk) where i.order_id=%s", (O,)) == [('cat_a',)], 'первый срез: продажа в категории A')
    check(query("select is_incomplete from dds.dim_product where product_id=%s and is_current", (P2,)) == [(True,)], 'товар только из order_items остаётся отдельной неполной сущностью')
    check(query("select p.is_incomplete from dds.fct_order_item i join dds.dim_product p using (product_sk) where i.order_id=%s", (O2,)) == [(True,)], 'до прихода атрибутов факт ссылается на неполную версию')
    check(query("select customer_sk, customer_hk, customer_city from dds.fct_order where order_id=%s", (O5,)) == [('0' * 32, '0' * 32, None)], 'нет покупателя и географии профиля — Unknown HK/SK и NULL')
    check(query("select customer_city from dds.fct_order where order_id=%s", (O,)) == [('Town',)], 'география первого профиля')
    build('02_repeat')
    check(initial == fingerprints(), 'повторная сборка: все DDS подписи равны, build_info сравнивается по отсечке')
    check(initial_mart == mart_fingerprints(), 'повторная сборка: подписи всех mart равны')
    batch(2)
    build('03_changes')
    check(query("select revenue_amount from mart.category_sales where category_key='cat_a'") == initial_a_revenue == [(100,)], 'mart: A → B не меняет выручку старой категории A')
    check(query("select revenue_amount from mart.category_sales where category_key='cat_b'") == [(130,)], 'mart: новая продажа 100 и поздний товар 30 относятся к B')
    check(query("select count(*) = count(distinct category_key) from mart.category_sales") == [(True,)], 'mart: один код категории — одна строка')
    check(query("select seller_city, revenue_amount from mart.seller_sales where seller_id=%s", (S,)) == [('New', 200)], 'mart: переехавший продавец — одна строка с текущим городом и всей выручкой')
    check(query("select customer_state, orders_count from mart.delivery_by_state where customer_state in ('RJ','MG') order by customer_state") == [('MG', 2), ('RJ', 2)], 'mart: два старых заказа остаются в RJ, новые связи получают MG')
    check(query("select count(distinct product_id), sum(unit_count), max(product_unit_count) from mart.product_price_history where product_id=%s", (P,)) == [(1, 2, 2)], 'mart: смена категории не дробит постоянную историю цены товара')
    check(query("select p.category_code, p.is_current from dds.fct_order_item i join dds.dim_product p using (product_sk) where i.order_id=%s", (O,)) == [('cat_a', False)], 'после A → B прежняя продажа остаётся в A на нетекущем SK')
    check(query("select p.category_code from dds.fct_order_item i join dds.dim_product p on p.product_hk=i.product_hk and p.is_current where i.order_id=%s", (O,)) == [('cat_b',)], 'через постоянный HK прежняя продажа видна в текущей категории B')
    check(query("select p.category_code, p.is_current from dds.fct_order_item i join dds.dim_product p using (product_sk) where i.order_id=%s", (O4,)) == [('cat_b', True)], 'новая продажа после смены привязана к B')
    check(query("select count(*) from dds.dim_seller where seller_id=%s", (S,)) == [(2,)], 'смена адреса продавца создаёт две версии')
    check(query("select s.seller_city, s.is_current from dds.fct_order_item i join dds.dim_seller s using (seller_sk) where i.order_id=%s", (O,)) == [('Old', False)], 'старая продажа сохраняет старый адрес продавца')
    check(query("select s.seller_city, s.is_current from dds.fct_order_item i join dds.dim_seller s using (seller_sk) where i.order_id=%s", (O4,)) == [('New', True)], 'новая продажа получает новый адрес продавца')
    check(query("select is_deleted, payment_value from dds.fct_payment where order_id=%s and payment_sequential=2", (O,)) == [(True, 10)], 'исчезнувший платёж сохранён с последними мерами и флагом удаления')
    check(query("select payment_value from dds.fct_order where order_id=%s", (O,)) == [(100,)], 'сумма заказа исключает исчезнувший платёж')
    check(query("select customer_id, customer_zip_code_prefix, customer_city, customer_state from dds.fct_order where order_id=%s", (O,)) == [(C2, '33333', 'Moved', 'MG')], 'смена профиля меняет географию заказа на географию новой связи')
    check(query("select customer_zip_code_prefix, customer_city, customer_state from dds.fct_order where order_id in (%s,%s) order by order_id", (O2, O3)) == [('22222', 'Other', 'RJ')] * 2, 'переезд того же customer_id сохраняет адрес обоих старых заказов')
    check(query("select customer_zip_code_prefix, customer_city, customer_state from dds.fct_order where order_id=%s", (O4,)) == [('33333', 'Moved', 'MG')], 'новый заказ профиля в батче переезда получает новый адрес')
    check(query("select count(*) from dds.fct_review where review_id=%s and is_deleted", (R,)) == [(2,)], 'удаление отзыва помечает обе пары')
    check(query("select is_translation_missing from dds.dim_product where product_id=%s and is_current", (P,)) == [(True,)], 'известная категория без перевода сохраняется')
    check(query("select count(*) from dds.dim_product where product_id=%s and not is_incomplete and is_current", (P2,)) == [(1,)], 'поступившие атрибуты создают полную версию товара')
    check(query("select p.is_incomplete, p.product_weight_g from dds.fct_order_item i join dds.dim_product p using (product_sk) where i.order_id=%s", (O2,)) == [(False, 200)], 'поздний товар заменяет неполную привязку первой полной версией')
    check(query("select customer_city from dds.fct_order where order_id=%s", (O5,)) == [('Late',)], 'нет географии до bind_dts — первая строка профиля на срезе')
    check(query("select c.customer_unique_id, c.is_incomplete, o.customer_hk=c.customer_hk from dds.fct_order o join dds.dim_customer c using (customer_sk) where o.order_id=%s", (O5,)) == [('b' * 32, False, True)], 'нет версии на bind_dts — первая полная версия известного покупателя, согласованные HK/SK')
    build('04_past_slice', only=['path:models/dds', 'path:models/mart'], as_of='2026-01-01 00:00:00+00')
    check(initial == fingerprints(), 'прошлая отсечка полностью воспроизводит первоначальный DDS, включая категорию и профиль')
    check(query("select customer_city from dds.fct_order where order_id in (%s,%s) order by order_id", (O2, O3)) == [('Other',)] * 2, 'до батча переезда оба существующих заказа имеют старый адрес')
    check(query("select customer_id, customer_city from dds.fct_order where order_id=%s", (O,)) == [(C, 'Town')], 'до смены профиля заказ имеет прежний профиль и географию')
    check(initial_mart == mart_fingerprints(), 'прошлая отсечка воспроизводит все mart')
    batch(3)
    build('05_latest')
    check(query("select count(*), min(p.product_id) from dds.fct_order_item i join dds.dim_product p using (product_sk) where i.order_id=%s", (O,)) == [(1, P2)], 'смена товара не дублирует зерно позиции')
    check(query("select p.product_weight_g, p.valid_from::text from dds.fct_order_item i join dds.dim_product p using (product_sk) where i.order_id=%s", (O,)) == [(300, '2026-01-03 00:00:00+00')], 'смена товара задаёт новый bind_dts и версию нового товара на новую связь')
    check(query("select s.is_deleted, i.is_deleted from dds.fct_order_item i join dds.dim_seller s using (seller_sk) where i.order_id=%s", (O,)) == [(True, False)], 'удаление продавца не удаляет продажи')
    check(query("select count(*) from dds.dim_product where product_id=%s and category_code='cat_b' and not is_translation_missing and is_current", (P,)) == [(1,)], 'поздний перевод создаёт новую версию товара')
    check(query("select c.customer_unique_id from dds.fct_order o join dds.dim_customer c using (customer_sk) where o.order_id=%s", (O,)) == [(U2,)], 'effectivity профиля выбирает нового постоянного покупателя')
    check(query("select customer_city from dds.fct_order where order_id=%s", (O5,)) == [('Late',)], 'поздняя география остаётся первой на срезе после следующего переезда')
    check(query("select category_name_english from mart.category_sales where category_key='cat_b'") == [('Category B',)], 'mart: поздний перевод обновляет название категории без дробления кода')
    batch(4)
    build('06_return')
    check(query("select array_agg(category_code order by valid_from) from dds.dim_product where product_id=%s", (P,))[0][0] == ['cat_a', 'cat_b', 'cat_b', 'cat_a'], 'A → B → A сохраняет разные версии, включая поздний перевод B')
    check(query("select is_deleted from dds.dim_seller where seller_id=%s and is_current", (S,)) == [(False,)], 'возврат продавца снимает удаление')
    check(query("select p.product_weight_g, p.is_current from dds.fct_order_item i join dds.dim_product p using (product_sk) where i.order_id=%s", (O,)) == [(300, False)], 'новая связь сохраняет версию своего bind_dts после следующего изменения товара')
    check(query("select p.product_weight_g, p.is_current from dds.fct_order_item i join dds.dim_product p using (product_sk) where i.order_id=%s", (O2,)) == [(200, False)], 'позднее измерение остаётся на первой полной версии, а не на последней')
    check(query("select p.category_code, p.is_translation_missing from dds.fct_order_item i join dds.dim_product p using (product_sk) where i.order_id=%s", (O4,)) == [('cat_b', True)], 'поздний перевод и возврат категории не переписывают привязанную продажу')
    latest = fingerprints()
    latest_mart = mart_fingerprints()
    build('07_latest_repeat')
    check(latest == fingerprints(), 'повторный последний срез детерминирован')
    check(latest_mart == mart_fingerprints(), 'повторный последний срез: все mart детерминированы')
    build('08_full_refresh', full=True)
    check(latest == fingerprints(), 'ODS full-refresh + DDS равны incremental по всем подписям')
    check(latest_mart == mart_fingerprints(), 'ODS full-refresh + DDS + mart равны incremental по всем mart')
    (WORK / 'mart-fingerprints.json').write_text(json.dumps(latest_mart, sort_keys=True))
    (WORK / 'fingerprints.json').write_text(json.dumps(latest, sort_keys=True))
    for test, table, records in [
        ('stg_order_profile_conflicts', 'orders', [dict(ROWS['orders'][0], customer_id=C), dict(ROWS['orders'][0], customer_id=C2)]),
        ('stg_customer_profile_link_conflicts', 'customers', [dict(ROWS['customers'][0], customer_unique_id=U), dict(ROWS['customers'][0], customer_unique_id=U2)]),
    ]:
        before = fingerprints(include_metadata=True)
        before_mart = mart_fingerprints()
        insert(table, records, 5)
        results = build('09_' + test, expect_success=False)
        check(any(x['unique_id'].endswith('.' + test) and x['status'] == 'fail' for x in results['results']), 'конфликт связей блокируется: ' + test)
        check(before == fingerprints(include_metadata=True), 'конфликт staging сохраняет прежние DDS таблицы')
        check(before_mart == mart_fingerprints(), 'конфликт staging сохраняет прежние mart таблицы')
        query(sql.SQL("delete from raw.{} where _batch_id='review-5'").format(sql.Identifier(table)))
    before = fingerprints(include_metadata=True)
    before_mart = mart_fingerprints()
    insert('order_items', [dict(ROWS['order_items'][0], price='not-money')], 6)
    results = build('10_critical', expect_success=False)
    check(any(x['unique_id'].endswith('.err_cast_critical') and x['status'] == 'fail' for x in results['results']), 'gate err_cast_critical падает')
    expected_models = (before.keys() - {'seed_order_status', 'seed_payment_type', 'seed_br_state'}) | {
        'fct_order__active', 'fct_order_item__active', 'fct_payment__active', 'fct_review__active'}
    dds_nodes = [x for x in results['results'] if x['unique_id'].startswith('model.marketplace.')
                 and x['unique_id'].split('.')[-1] in expected_models]
    check(len(dds_nodes) == len(expected_models) and all(x['status'] == 'skipped' for x in dds_nodes), 'все модели DDS пропущены при критической ошибке')
    check(before == fingerprints(include_metadata=True), 'gate сохраняет все прежние DDS таблицы, включая build_info')
    mart_nodes = [x for x in results['results'] if x['unique_id'].startswith('model.marketplace.')
                  and x['unique_id'].split('.')[-1] in before_mart]
    check(len(mart_nodes) == len(before_mart) and all(x['status'] == 'skipped' for x in mart_nodes), 'все модели mart пропущены при критической ошибке')
    check(before_mart == mart_fingerprints(), 'gate сохраняет все прежние mart таблицы')
    print('DDS_ACCEPTANCE_PASSED; tables compared:', len(latest), flush=True)
finally:
    with admin.cursor() as c:
        c.execute(sql.SQL('drop database if exists {} with (force)').format(sql.Identifier(DB)))
    admin.close()
    print('Временная БД удалена; логи:', WORK, flush=True)
