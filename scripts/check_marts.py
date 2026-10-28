"""Regression checks for the checksum-pinned Olist fixture, not arbitrary new data."""
import os
from decimal import Decimal
import psycopg2

with psycopg2.connect(os.environ['DWH_DSN']) as conn, conn.cursor() as cur:
    checks = [
        ('select revenue, orders_count, average_order_with_freight from mart.order_summary',
         (Decimal('13221498.11'), 96478, Decimal('159.88'))),
        ('select customers_count, repeat_customers_count, repeat_pct from mart.customer_summary',
         (96096, 2997, Decimal('3.1188'))),
        ('select count(*), sum(revenue) from mart.monthly_sales', (24, Decimal('13221498.11'))),
        ("select orders_count, revenue from mart.monthly_sales where month_start='2016-11-01'", (0, Decimal('0'))),
        ('select count(*),sum(revenue) from mart.category_sales', (74,Decimal('13221498.11'))),
        ('select max(revenue) from mart.seller_sales', (Decimal('226987.93'),)),
    ]
    for query, expected in checks:
        cur.execute(query)
        actual = cur.fetchone()
        if actual != expected:
            raise AssertionError(f'{query}: {actual} != {expected}')
    print(f'Olist mart controls: {len(checks)}/{len(checks)} passed')
