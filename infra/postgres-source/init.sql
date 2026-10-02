-- Source OLTP schema for the sample online shop.
-- Debezium reads changes to these tables from the Postgres WAL (logical decoding, pgoutput).

CREATE TABLE customers (
    customer_id  SERIAL PRIMARY KEY,
    email        TEXT        NOT NULL UNIQUE,
    full_name    TEXT        NOT NULL,
    country      CHAR(2)     NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id   SERIAL PRIMARY KEY,
    sku          TEXT          NOT NULL UNIQUE,
    name         TEXT          NOT NULL,
    category     TEXT          NOT NULL,
    unit_price   NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    is_active    BOOLEAN       NOT NULL DEFAULT TRUE,
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INT         NOT NULL REFERENCES customers(customer_id),
    status       TEXT        NOT NULL CHECK (status IN ('pending','paid','shipped','delivered','cancelled')),
    currency     CHAR(3)     NOT NULL,
    order_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id      INT           NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id    INT           NOT NULL REFERENCES products(product_id),
    quantity      INT           NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(10,2) NOT NULL
);

-- Signal table: lets us trigger Debezium incremental snapshots (backfills) with an INSERT.
CREATE TABLE debezium_signal (
    id   VARCHAR(42) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data VARCHAR(2048)
);

-- Keep updated_at honest on every UPDATE.
CREATE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customers_touch BEFORE UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER products_touch  BEFORE UPDATE ON products  FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER orders_touch    BEFORE UPDATE ON orders    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- REPLICA IDENTITY FULL: UPDATE/DELETE events carry the complete "before" row, not just the key.
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- Publication Debezium subscribes to (autocreate is disabled in the connector config).
CREATE PUBLICATION dbz_shop_publication
    FOR TABLE customers, products, orders, order_items, debezium_signal;

-- ── Seed data (deterministic) ────────────────────────────────────────────────
SELECT setseed(0.42);

INSERT INTO customers (email, full_name, country, created_at, updated_at)
SELECT
    'customer' || g || '@example.com',
    (ARRAY['Ayu','Budi','Chen','Diana','Eko','Fatima','Gita','Hans','Ines','Joko'])[1 + (g % 10)]
        || ' ' ||
    (ARRAY['Santoso','Wijaya','Tan','Smith','Muller','Garcia','Lim','Nguyen','Rossi','Kumar'])[1 + ((g / 10) % 10)],
    (ARRAY['ID','SG','MY','US','GB','DE','AU','JP'])[1 + floor(random() * 8)::int],
    now() - (random() * interval '365 days'),
    now()
FROM generate_series(1, 200) AS g;

INSERT INTO products (sku, name, category, unit_price)
SELECT
    'SKU-' || lpad(g::text, 4, '0'),
    c.category || ' item ' || g,
    c.category,
    round((5 + random() * 495)::numeric, 2)
FROM generate_series(1, 50) AS g
CROSS JOIN LATERAL (
    SELECT (ARRAY['Electronics','Home','Fashion','Sports','Books'])[1 + (g % 5)] AS category
) AS c;

INSERT INTO orders (customer_id, status, currency, order_ts)
SELECT
    1 + floor(random() * 200)::int,
    (ARRAY['pending','paid','shipped','delivered','delivered','delivered','cancelled'])[1 + floor(random() * 7)::int],
    (ARRAY['USD','EUR','GBP','IDR','SGD'])[1 + floor(random() * 5)::int],
    now() - (random() * interval '90 days')
FROM generate_series(1, 500);

-- 1–3 line items per order; "o.order_id * 0" correlates the lateral call so it re-rolls per order.
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT x.order_id, p.product_id, x.quantity, p.unit_price
FROM (
    SELECT o.order_id,
           1 + floor(random() * 50)::int AS product_id,
           1 + floor(random() * 4)::int  AS quantity
    FROM orders o
    CROSS JOIN LATERAL generate_series(1, 1 + floor(random() * 3 + o.order_id * 0)::int) AS n
) AS x
JOIN products p USING (product_id);
