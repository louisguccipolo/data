import os
from superset.app import create_app
from superset.extensions import db
app = create_app()
with app.app_context():
    from superset.models.core import Database
    sm = app.appbuilder.sm
    if not sm.find_user(username='admin'):
        sm.add_user('admin', 'Local', 'Admin', 'admin@localhost.local', sm.find_role('Admin'), os.environ['SUPERSET_ADMIN_PASSWORD'])
    database = db.session.query(Database).filter_by(database_name='Marketplace DWH').one_or_none()
    if database is None:
        database = Database(database_name='Marketplace DWH')
        db.session.add(database)
    database.set_sqlalchemy_uri('postgresql+psycopg2://analyst_user:' + os.environ['DWH_ANALYST_PASSWORD'] + '@dwh:5432/dwh')
    database.expose_in_sqllab = True
    db.session.commit()
    from superset.connectors.sqla.models import SqlaTable
    from sqlalchemy import inspect
    with database.get_sqla_engine() as engine:
        available = set(inspect(engine).get_table_names(schema="ods") + inspect(engine).get_view_names(schema="ods"))
    for name in ('stg_orders', 'stg_order_items', 'stg_order_payments', 'stg_products', 'stg_sellers', 'stg_product_category_translation'):
        if name not in available:
            continue
        dataset = db.session.query(SqlaTable).filter_by(database_id=database.id, schema='ods', table_name=name).one_or_none()
        if dataset is None:
            dataset = SqlaTable(database=database, schema='ods', table_name=name)
            db.session.add(dataset)
        try:
            dataset.fetch_metadata()
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise

    for name in ('monthly_sales', 'seller_sales', 'order_summary', 'customer_summary', 'category_sales'):
        with database.get_sqla_engine() as engine:
            if not inspect(engine).has_table(name, schema='mart'):
                continue
        dataset = db.session.query(SqlaTable).filter_by(database_id=database.id, schema='mart', table_name=name).one_or_none()
        if dataset is None:
            dataset = SqlaTable(database=database, schema='mart', table_name=name)
            db.session.add(dataset)
        dataset.fetch_metadata()
        db.session.commit()
