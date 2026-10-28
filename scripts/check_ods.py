"""Независимая приемка dbt ODS на синтетических батчах.

Запуск из корня локального стенда:
    docker compose exec -T airflow python - < scripts/check_ods.py

Все трансформации выполняет dbt. Этот harness создает отдельную временную БД,
копирует в нее только DDL raw, вставляет тестовые строки и проверяет результат.
Живые source/raw/ODS не изменяются. Нужны права CREATE DATABASE у DWH_DSN;
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

DB = 'ods_review_' + uuid.uuid4().hex[:12]
WORK = Path(tempfile.mkdtemp(prefix='ods-review-'))
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

def build(label, full=False, expect_success=True, only=None):
    args = ['dbt', '--no-use-colors', '--log-path', str(WORK / 'logs'), 'build',
            '--project-dir', '/opt/project/dbt', '--profiles-dir', str(WORK),
            '--target-path', str(WORK / 'target'), '--no-partial-parse']
    if full:
        args.append('--full-refresh')
    if only:
        args.extend(['--select', *only])
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

def batch(n):
    import copy
    rows = copy.deepcopy(ROWS)
    if n >= 2:
        rows['orders'][0]['order_status'] = 'delivered'
        rows['order_items'][0]['product_id'] = P2
        rows['order_items'][0]['price'] = '110.00'
        rows['order_payments'] = [dict(order_id=O, payment_sequential='1', payment_type='credit_card', payment_installments='1', payment_value='120.00'), ROWS['order_payments'][2]]
    if n == 2:
        rows['sellers'] = [rows['sellers'][1]]
        rows['products'][0]['product_category_name'] = 'cat'
    for table, records in rows.items():
        insert(table, records, n)
    for table, keys in KEYS.items():
        insert(table + '__keys', [{k: r.get(k) for k in keys} for r in rows[table]], n)

def fingerprints():
    tables = query("select tablename from pg_tables where schemaname='ods' order by tablename")
    result = {}
    for (table,) in tables:
        result[table] = query(sql.SQL("select count(*), md5(coalesce(string_agg(md5(t::text), '' order by md5(t::text)), '')) from ods.{} t").format(sql.Identifier(table)))[0]
    return result

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
    print('Isolated review database:', DB, 'logs:', WORK, flush=True)
    batch(1)
    build('01_initial')
    check(query('select count(*) from ods.hub_customer')[0][0] == 1, 'customer_unique_id does not become customer profile id')
    check(query('select count(*) from ods.hub_customer_profile')[0][0] == 2, 'two customer profiles remain separate')
    check(query('select count(*) from ods.link_order_review')[0][0] == 2 and query('select count(*) from ods.hub_review')[0][0] == 1, 'many-to-many review links preserve one review entity')
    initial = fingerprints()
    build('02_repeat')
    check(initial == fingerprints(), 'repeat build inserts no duplicate versions')
    batch(2)
    build('03_changes')
    check(query('select count(*) from ods.sat_order_status')[0][0] == 3, 'status transition appends a version')
    check(query('select count(*) from ods.sat_order')[0][0] == 2, 'unchanged order attributes are collapsed')
    check(query('select count(*) from ods.sat_order_payment')[0][0] == 4, 'changed payment parent stores its new complete set')
    check(query('select count(*) from ods.eff_seller where is_deleted')[0][0] == 1, 'missing seller recorded as deleted')
    check(query('select count(*) from ods.eff_order_item where is_deleted')[0][0] == 1, 'changed product retires the old item link')
    batch(3)
    build('04_reappearance')
    check(query('select count(*) from ods.eff_seller')[0][0] == 4, 'seller delete and reappearance are both recorded')
    check(query('select count(*) from ods.sat_order_payment')[0][0] == 4, 'unchanged payment parent sets are collapsed')
    check(query('select count(*) from ods.eff_product_category')[0][0] == 6, 'category A-to-B-to-A records activation and retirement events')
    category_events = query('select is_deleted, load_dts from ods.eff_product_category where category_hk=repeat(\'0\',32) order by load_dts')
    check([r[0] for r in category_events] == [False, True, False], 'original unknown category is active, retired, then active again')
    incremental = fingerprints()
    build('05_full_refresh', full=True)
    actual = fingerprints()
    check(incremental == actual, 'incremental history equals full refresh for every ODS table')
    (WORK / 'fingerprints.json').write_text(json.dumps(actual, sort_keys=True))
    print('ODS tables compared:', len(actual), flush=True)
    # A malformed noncritical value must survive as null plus a warning entry.
    bad = dict(ROWS['products'][1], product_weight_g='not-a-number')
    insert('products', [bad], 4)
    build('06_noncritical')
    check(query("select count(*) from ods.err_cast where column_name='product_weight_g' and severity='warn'")[0][0] == 1, 'noncritical casting error is logged without dropping the row')
    # Critical cast error must stop a build, preserving the raw text.
    bad_item = dict(ROWS['order_items'][0], price='not-money')
    insert('order_items', [bad_item], 5)
    results = build('07_critical', expect_success=False)
    check(any(r['unique_id'].endswith('.err_cast_critical') and r['status'] == 'fail' for r in results['results']), 'critical casting test actually fails')
    check(query("select count(*) from ods.err_cast where column_name='price' and original_value='not-money'")[0][0] == 1, 'critical original value and context remain in error journal')
    insert('products', [dict(ROWS['products'][0], product_category_name='cat')], 3)
    conflicts = build('08_category_conflict', expect_success=False, only=['stg_product_category_conflicts'])
    check(any(r['unique_id'].endswith('.stg_product_category_conflicts') and r['status'] == 'fail' for r in conflicts['results']), 'two different categories for a product at the same batch timestamp fail explicitly')
    print('INDEPENDENT_ACCEPTANCE_PASSED', flush=True)
finally:
    with admin.cursor() as c:
        c.execute(sql.SQL('drop database if exists {} with (force)').format(sql.Identifier(DB)))
    admin.close()
    print('Temporary database removed; logs:', WORK, flush=True)
