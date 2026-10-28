"""Живой трафик в OLTP (новые заказы, статусы, отзывы, SCD2-события) — для демо инкрементов.
Если бэкенд генерит реальный трафик — просто не включайте этот DAG."""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


@dag(schedule="*/10 * * * *", start_date=datetime(2026, 1, 1), catchup=False, tags=["marketplace", "demo"])
def oltp_simulator():
    BashOperator(task_id="tick", bash_command="python /opt/project/oltp/seed.py tick --new-orders 40")


oltp_simulator()
