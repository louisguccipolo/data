"""Генератор данных для OLTP маркетплейса.

  python oltp/seed.py init  [--users 5000 --sellers 200 --products 3000 --orders 40000 --days 365]
      Заливает историю в пустую базу.
  python oltp/seed.py tick  [--new-orders 50]
      Живой трафик: новые заказы, продвижение статусов, отзывы, смена города у части
      покупателей (чтобы было что ловить SCD2). Вызывается DAG-ом oltp_simulator.
"""

import argparse
import math
import os
import random
from datetime import datetime, timedelta, timezone

import psycopg2
from faker import Faker
from psycopg2.extras import execute_values

fake = Faker("ru_RU")

CITIES = [
    ("Москва", "Москва", 30), ("Санкт-Петербург", "Санкт-Петербург", 14),
    ("Новосибирск", "Новосибирская область", 5), ("Екатеринбург", "Свердловская область", 5),
    ("Казань", "Республика Татарстан", 5), ("Нижний Новгород", "Нижегородская область", 4),
    ("Красноярск", "Красноярский край", 3), ("Самара", "Самарская область", 3),
    ("Уфа", "Республика Башкортостан", 3), ("Ростов-на-Дону", "Ростовская область", 4),
    ("Краснодар", "Краснодарский край", 5), ("Воронеж", "Воронежская область", 3),
    ("Пермь", "Пермский край", 3), ("Владивосток", "Приморский край", 2),
    ("Калининград", "Калининградская область", 2),
]
# L1 -> [(L2, средняя цена)]
CATEGORIES = {
    "Электроника": [("Смартфоны", 25000), ("Ноутбуки", 60000), ("Наушники", 4000), ("Аксессуары", 900)],
    "Одежда": [("Женская одежда", 2500), ("Мужская одежда", 2800), ("Обувь", 4500), ("Детская одежда", 1500)],
    "Дом и сад": [("Кухня", 1800), ("Текстиль", 1500), ("Инструменты", 3500)],
    "Красота": [("Уход за кожей", 900), ("Парфюмерия", 3500), ("Макияж", 700)],
    "Продукты": [("Снеки", 250), ("Напитки", 180), ("Бакалея", 300)],
    "Детские товары": [("Игрушки", 1500), ("Подгузники", 1600), ("Детское питание", 400)],
}
BRANDS = ["Aurora", "Nord", "Сибирь", "Polar", "Vega", "Ладога", "Orion", "Baikal", "Tundra", "Kedr",
          "Volna", "Zarya", "Lumen", "Pixel", "Комфорт", "Домовой", "Urban", "Taiga", "Sputnik", "Altai"]
WAREHOUSES = [("Склад Подольск", "Москва"), ("Склад Шушары", "Санкт-Петербург"),
              ("Склад Казань", "Казань"), ("Склад Екатеринбург", "Екатеринбург"),
              ("Склад Новосибирск", "Новосибирск"), ("Склад Краснодар", "Краснодар")]
PAYMENT = (["card", "sbp", "installment", "cash"], [55, 30, 10, 5])


def connect():
    return psycopg2.connect(os.environ.get("OLTP_DSN", "host=localhost port=15432 dbname=marketplace user=app"))


def now():
    return datetime.now(timezone.utc)


def lifecycle(created, rng, horizon):
    """Статус и таймстемпы этапов заказа; события позже horizon не происходят."""
    ts = {"paid_at": None, "shipped_at": None, "delivered_at": None, "cancelled_at": None, "returned_at": None}
    status = "created"
    t = created + timedelta(minutes=rng.uniform(1, 90))
    if rng.random() < 0.07:
        if t <= horizon:
            return "cancelled", {**ts, "cancelled_at": t}
        return status, ts
    for step, field, delay_h in (("paid", "paid_at", (0.02, 1.5)), ("shipped", "shipped_at", (4, 48)),
                                 ("delivered", "delivered_at", (12, 120))):
        if t > horizon:
            return status, ts
        status, ts[field] = step, t
        if step == "paid" and rng.random() < 0.03:  # отмена после оплаты
            t2 = t + timedelta(hours=rng.uniform(1, 24))
            if t2 <= horizon:
                return "cancelled", {**ts, "cancelled_at": t2}
        t = t + timedelta(hours=rng.uniform(*delay_h))
    if rng.random() < 0.04:
        t2 = ts["delivered_at"] + timedelta(days=rng.uniform(1, 14))
        if t2 <= horizon:
            status, ts["returned_at"] = "returned", t2
    return status, ts


def init(args):
    rng = random.Random(42)
    Faker.seed(42)
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM orders")
    if cur.fetchone()[0]:
        raise SystemExit("orders не пуста — init только для пустой базы (make nuke-oltp для пересоздания)")

    end = now()
    start = end - timedelta(days=args.days)

    execute_values(cur, "INSERT INTO cities (city_id, name, region) VALUES %s",
                   [(i + 1, c, r) for i, (c, r, _) in enumerate(CITIES)])
    city_ids = list(range(1, len(CITIES) + 1))
    city_w = [w for *_, w in CITIES]
    city_by_name = {c: i + 1 for i, (c, _, _) in enumerate(CITIES)}

    cats, l2 = [], []  # l2: (category_id, avg_price)
    cid = 0
    for l1_name, children in CATEGORIES.items():
        cid += 1
        parent = cid
        cats.append((parent, None, l1_name))
        for name, price in children:
            cid += 1
            cats.append((cid, parent, name))
            l2.append((cid, price))
    execute_values(cur, "INSERT INTO categories (category_id, parent_id, name) VALUES %s", cats)

    execute_values(cur, "INSERT INTO warehouses (warehouse_id, name, city_id) VALUES %s",
                   [(i + 1, n, city_by_name[c]) for i, (n, c) in enumerate(WAREHOUSES)])
    wh_by_city = {city_by_name[c]: i + 1 for i, (_, c) in enumerate(WAREHOUSES)}

    def rand_ts(lo, hi):
        return lo + timedelta(seconds=rng.uniform(0, (hi - lo).total_seconds()))

    sellers = []
    for sid in range(1, args.sellers + 1):
        created = rand_ts(start - timedelta(days=365), start + timedelta(days=args.days * 0.6))
        sellers.append((sid, f"{fake.word().capitalize()} {rng.choice(['Store', 'Shop', 'Market', 'Маркет', 'Лавка'])}",
                        f"ООО «{fake.company().replace('«', '').replace('»', '')}»", fake.businesses_inn(),
                        fake.checking_account(), fake.company_email(), fake.phone_number(),
                        rng.choices(city_ids, city_w)[0], created, created))
    execute_values(cur, """INSERT INTO sellers (seller_id, shop_name, legal_name, inn, bank_account, contact_email,
                           contact_phone, city_id, created_at, updated_at) VALUES %s""", sellers)
    seller_created = {s[0]: s[8] for s in sellers}

    users = []
    emails = set()
    for uid in range(1, args.users + 1):
        gender = rng.choice("MF")
        name = fake.name_male() if gender == "M" else fake.name_female()
        email = f"{fake.user_name()}{uid}@{rng.choice(['mail.ru', 'yandex.ru', 'gmail.com', 'bk.ru'])}"
        emails.add(email)
        created = rand_ts(start - timedelta(days=365), end - timedelta(days=1))
        users.append((uid, email, fake.phone_number(), name,
                      fake.date_of_birth(minimum_age=16, maximum_age=70), gender,
                      rng.choices(city_ids, city_w)[0], created, created))
    execute_values(cur, """INSERT INTO users (user_id, email, phone, full_name, birth_date, gender, city_id,
                           created_at, updated_at) VALUES %s""", users)
    user_city = {u[0]: u[6] for u in users}
    user_created = {u[0]: u[7] for u in users}

    products = []
    for pid in range(1, args.products + 1):
        cat, avg = rng.choice(l2)
        sid = rng.randint(1, args.sellers)
        price = round(max(49, rng.lognormvariate(math.log(avg), 0.5)), -1) - 1
        created = max(seller_created[sid], start - timedelta(days=30))
        cat_name = next(n for i, _, n in cats if i == cat)
        products.append((pid, sid, cat, f"{cat_name[:-1] if cat_name.endswith('и') else cat_name} {fake.word()}",
                         rng.choice(BRANDS), price, created, created))
    execute_values(cur, """INSERT INTO products (product_id, seller_id, category_id, name, brand, price,
                           created_at, updated_at) VALUES %s""", products)
    # популярность товаров — степенной закон
    prod_w = [1 / (i + 1) ** 0.8 for i in range(len(products))]
    rng.shuffle(prod_w)

    orders, items, reviews = [], [], []
    oi_id = rv_id = 0
    total_sec = (end - start).total_seconds()
    for oid in range(1, args.orders + 1):
        # рост бизнеса во времени + недельная сезонность (пик в выходные)
        while True:
            x = rng.random()
            created = start + timedelta(seconds=total_sec * math.sqrt(x))
            if rng.random() < (1.0 if created.weekday() >= 5 else 0.8):
                break
        uid = rng.randint(1, args.users)
        if user_created[uid] > created:
            created = user_created[uid] + timedelta(hours=rng.uniform(1, 72))
            if created > end:
                created = end - timedelta(minutes=rng.uniform(5, 600))
        status, ts = lifecycle(created, rng, end)
        city = user_city[uid]
        wh = wh_by_city.get(city, rng.choice(list(wh_by_city.values())))
        pay = rng.choices(*PAYMENT)[0]
        last = max([created] + [v for v in ts.values() if v])
        orders.append((oid, uid, wh, city, status, pay, created, ts["paid_at"], ts["shipped_at"],
                       ts["delivered_at"], ts["cancelled_at"], ts["returned_at"], last))
        for p in rng.choices(products, prod_w, k=rng.choices([1, 2, 3, 4], [55, 25, 12, 8])[0]):
            oi_id += 1
            qty = rng.choices([1, 2, 3], [80, 15, 5])[0]
            disc = round(p[5] * qty * rng.choice([0, 0, 0, 0.05, 0.1, 0.2]), 2)
            items.append((oi_id, oid, p[0], p[1], qty, p[5], disc, created, created))
            if ts["delivered_at"] and rng.random() < 0.3:
                rv_id += 1
                rts = ts["delivered_at"] + timedelta(days=rng.uniform(0.5, 10))
                if rts < end:
                    rating = rng.choices([1, 2, 3, 4, 5], [5, 5, 10, 25, 55])[0]
                    reviews.append((rv_id, oi_id, uid, p[0], rating, fake.sentence(nb_words=8), rts, rts))

    execute_values(cur, """INSERT INTO orders (order_id, user_id, warehouse_id, delivery_city_id, status,
                           payment_method, created_at, paid_at, shipped_at, delivered_at, cancelled_at,
                           returned_at, updated_at) VALUES %s""", orders, page_size=5000)
    execute_values(cur, """INSERT INTO order_items (order_item_id, order_id, product_id, seller_id, quantity,
                           unit_price, discount, created_at, updated_at) VALUES %s""", items, page_size=5000)
    execute_values(cur, """INSERT INTO reviews (review_id, order_item_id, user_id, product_id, rating, body,
                           created_at, updated_at) VALUES %s""", reviews, page_size=5000)

    for t, pk in [("cities", "city_id"), ("categories", "category_id"), ("warehouses", "warehouse_id"),
                  ("sellers", "seller_id"), ("users", "user_id"), ("products", "product_id"),
                  ("orders", "order_id"), ("order_items", "order_item_id"), ("reviews", "review_id")]:
        cur.execute(f"SELECT setval(pg_get_serial_sequence('{t}', '{pk}'), (SELECT max({pk}) FROM {t}))")
    conn.commit()
    print(f"users={len(users)} sellers={len(sellers)} products={len(products)} "
          f"orders={len(orders)} items={len(items)} reviews={len(reviews)}")


def tick(args):
    rng = random.Random()
    conn = connect()
    cur = conn.cursor()

    # 1. Новые заказы
    cur.execute("SELECT user_id, city_id FROM users WHERE status = 'active' ORDER BY random() LIMIT %s",
                (args.new_orders,))
    buyers = cur.fetchall()
    cur.execute("SELECT warehouse_id, city_id FROM warehouses")
    wh_by_city = {c: w for w, c in cur.fetchall()}
    cur.execute("SELECT product_id, seller_id, price FROM products WHERE is_active ORDER BY random() LIMIT 500")
    catalog = cur.fetchall()
    for uid, city in buyers:
        cur.execute("""INSERT INTO orders (user_id, warehouse_id, delivery_city_id, payment_method)
                       VALUES (%s, %s, %s, %s) RETURNING order_id""",
                    (uid, wh_by_city.get(city, rng.choice(list(wh_by_city.values()))), city,
                     rng.choices(*PAYMENT)[0]))
        oid = cur.fetchone()[0]
        for pid, sid, price in rng.sample(catalog, rng.choices([1, 2, 3], [60, 30, 10])[0]):
            cur.execute("""INSERT INTO order_items (order_id, product_id, seller_id, quantity, unit_price)
                           VALUES (%s, %s, %s, %s, %s)""", (oid, pid, sid, rng.choice([1, 1, 1, 2]), price))

    # 2. Продвижение статусов
    cur.execute("""UPDATE orders SET status = 'paid', paid_at = now()
                   WHERE status = 'created' AND created_at < now() - interval '1 minute' AND random() < 0.8""")
    cur.execute("""UPDATE orders SET status = 'cancelled', cancelled_at = now()
                   WHERE status = 'created' AND created_at < now() - interval '1 minute' AND random() < 0.1""")
    cur.execute("UPDATE orders SET status = 'shipped', shipped_at = now() WHERE status = 'paid' AND random() < 0.5")
    cur.execute("""UPDATE orders SET status = 'delivered', delivered_at = now()
                   WHERE status = 'shipped' AND random() < 0.4""")
    cur.execute("""UPDATE orders SET status = 'returned', returned_at = now()
                   WHERE status = 'delivered' AND delivered_at > now() - interval '14 days' AND random() < 0.005""")

    # 3. Отзывы на доставленное
    cur.execute("""INSERT INTO reviews (order_item_id, user_id, product_id, rating, body)
                   SELECT oi.order_item_id, o.user_id, oi.product_id,
                          (ARRAY[1,2,3,4,4,5,5,5,5,5])[1 + floor(random() * 10)::int], 'Отзыв'
                   FROM order_items oi JOIN orders o USING (order_id)
                   LEFT JOIN reviews r USING (order_item_id)
                   WHERE o.status = 'delivered' AND r.review_id IS NULL AND random() < 0.05""")

    # 4. SCD2-события: переезд покупателей, смена названия магазина
    cur.execute("""UPDATE users SET city_id = (SELECT city_id FROM cities ORDER BY random() LIMIT 1)
                   WHERE user_id IN (SELECT user_id FROM users ORDER BY random() LIMIT 3)""")
    cur.execute("""UPDATE sellers SET shop_name = shop_name || ' Pro'
                   WHERE seller_id IN (SELECT seller_id FROM sellers WHERE shop_name NOT LIKE '%% Pro'
                                       ORDER BY random() LIMIT 1)""")
    conn.commit()
    print(f"tick: +{len(buyers)} orders")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("init")
    p.add_argument("--users", type=int, default=5000)
    p.add_argument("--sellers", type=int, default=200)
    p.add_argument("--products", type=int, default=3000)
    p.add_argument("--orders", type=int, default=40000)
    p.add_argument("--days", type=int, default=365)
    t = sub.add_parser("tick")
    t.add_argument("--new-orders", type=int, default=50)
    a = ap.parse_args()
    init(a) if a.cmd == "init" else tick(a)
