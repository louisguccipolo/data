-- OLTP-схема маркетплейса (Postgres 16). Владелец — бэкенд.
-- Контракт с DWH:
--   * у каждой таблицы есть PK и updated_at — по нему идёт инкрементальная выгрузка;
--   * updated_at обновляется триггером, бэкенду не нужно помнить об этом;
--   * удаления — только soft delete через status (hard delete DWH не увидит);
--   * enum-ы — text + CHECK, а не CREATE TYPE (типы 1:1 переносятся в Cloudberry);
--   * новые колонки можно добавлять свободно — raw-слой подхватит их сам.

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE cities (
    city_id     serial PRIMARY KEY,
    name        text NOT NULL,
    region      text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Покупатели. full_name / email / phone / birth_date — PII.
CREATE TABLE users (
    user_id     bigserial PRIMARY KEY,
    email       text NOT NULL UNIQUE,
    phone       text,
    full_name   text NOT NULL,
    birth_date  date,
    gender      char(1) CHECK (gender IN ('M', 'F')),
    city_id     int REFERENCES cities,
    status      text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'blocked', 'deleted')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Продавцы. legal_name / inn / bank_account / contact_* — чувствительные данные.
CREATE TABLE sellers (
    seller_id     serial PRIMARY KEY,
    shop_name     text NOT NULL,
    legal_name    text NOT NULL,
    inn           varchar(12) NOT NULL,
    bank_account  varchar(20) NOT NULL,
    contact_email text,
    contact_phone text,
    city_id       int REFERENCES cities,
    status        text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'closed')),
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Дерево категорий (parent_id IS NULL — корень).
CREATE TABLE categories (
    category_id serial PRIMARY KEY,
    parent_id   int REFERENCES categories,
    name        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id  bigserial PRIMARY KEY,
    seller_id   int NOT NULL REFERENCES sellers,
    category_id int NOT NULL REFERENCES categories,
    name        text NOT NULL,
    brand       text,
    price       numeric(12, 2) NOT NULL CHECK (price >= 0),
    is_active   boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE warehouses (
    warehouse_id serial PRIMARY KEY,
    name         text NOT NULL,
    city_id      int NOT NULL REFERENCES cities,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Заказ. Таймстемпы этапов — источник для accumulating snapshot в DWH.
CREATE TABLE orders (
    order_id         bigserial PRIMARY KEY,
    user_id          bigint NOT NULL REFERENCES users,
    warehouse_id     int REFERENCES warehouses,
    delivery_city_id int REFERENCES cities,
    status           text NOT NULL DEFAULT 'created'
                     CHECK (status IN ('created', 'paid', 'shipped', 'delivered', 'cancelled', 'returned')),
    payment_method   text CHECK (payment_method IN ('card', 'sbp', 'installment', 'cash')),
    created_at       timestamptz NOT NULL DEFAULT now(),
    paid_at          timestamptz,
    shipped_at       timestamptz,
    delivered_at     timestamptz,
    cancelled_at     timestamptz,
    returned_at      timestamptz,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Строка заказа. Цена фиксируется на момент покупки; seller_id денормализован намеренно.
CREATE TABLE order_items (
    order_item_id bigserial PRIMARY KEY,
    order_id      bigint NOT NULL REFERENCES orders,
    product_id    bigint NOT NULL REFERENCES products,
    seller_id     int NOT NULL REFERENCES sellers,
    quantity      int NOT NULL CHECK (quantity > 0),
    unit_price    numeric(12, 2) NOT NULL,
    discount      numeric(12, 2) NOT NULL DEFAULT 0,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reviews (
    review_id     bigserial PRIMARY KEY,
    order_item_id bigint NOT NULL UNIQUE REFERENCES order_items,
    user_id       bigint NOT NULL REFERENCES users,
    product_id    bigint NOT NULL REFERENCES products,
    rating        smallint NOT NULL CHECK (rating BETWEEN 1 AND 5),
    body          text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ON orders (user_id);
CREATE INDEX ON orders (updated_at);
CREATE INDEX ON order_items (order_id);
CREATE INDEX ON order_items (updated_at);
CREATE INDEX ON products (seller_id);
CREATE INDEX ON users (updated_at);

DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['cities','users','sellers','categories','products','warehouses','orders','order_items','reviews']
    LOOP
        EXECUTE format('CREATE TRIGGER trg_%1$s_updated_at BEFORE UPDATE ON %1$I
                        FOR EACH ROW EXECUTE FUNCTION set_updated_at()', t);
    END LOOP;
END $$;
