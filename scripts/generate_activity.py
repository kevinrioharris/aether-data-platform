"""Simulate live shop traffic so Debezium has inserts, updates, and deletes to capture.

    python scripts/generate_activity.py --events-per-sec 5 --duration 300
"""

from __future__ import annotations

import argparse
import os
import random
import time

import psycopg

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

NEXT_STATUS = {"pending": "paid", "paid": "shipped", "shipped": "delivered"}
COUNTRIES = ["ID", "SG", "MY", "US", "GB", "DE", "AU", "JP"]
# Storefront conversion from USD list price to the order currency (roughly market rates).
CURRENCY_RATES = {"USD": 1.0, "EUR": 0.86, "GBP": 0.74, "IDR": 17700.0, "SGD": 1.27}


def new_order(cur) -> str:
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    (customer_id,) = cur.fetchone()
    currency = random.choice(list(CURRENCY_RATES))
    cur.execute(
        "INSERT INTO orders (customer_id, status, currency) VALUES (%s, 'pending', %s) RETURNING order_id",
        (customer_id, currency),
    )
    (order_id,) = cur.fetchone()
    cur.execute(
        """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        SELECT %s, product_id, 1 + floor(random() * 4)::int, round(unit_price * %s::numeric, 2)
        FROM products WHERE is_active ORDER BY random() LIMIT %s
        """,
        (order_id, CURRENCY_RATES[currency], random.randint(1, 3)),
    )
    return f"insert order {order_id}"


def advance_order(cur) -> str:
    cur.execute(
        "SELECT order_id, status FROM orders WHERE status IN ('pending','paid','shipped') "
        "ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if row is None:
        return new_order(cur)
    order_id, status = row
    target = "cancelled" if status == "pending" and random.random() < 0.2 else NEXT_STATUS[status]
    cur.execute("UPDATE orders SET status = %s WHERE order_id = %s", (target, order_id))
    return f"update order {order_id}: {status} → {target}"


def reprice_product(cur) -> str:
    factor = random.uniform(0.85, 1.15)
    cur.execute(
        "UPDATE products SET unit_price = round(unit_price * %s::numeric, 2) "
        "WHERE product_id = (SELECT product_id FROM products ORDER BY random() LIMIT 1) RETURNING product_id",
        (factor,),
    )
    (product_id,) = cur.fetchone()
    return f"reprice product {product_id} ×{factor:.2f}"


def new_or_moved_customer(cur) -> str:
    if random.random() < 0.5:
        cur.execute(
            "INSERT INTO customers (email, full_name, country) "
            "VALUES ('new' || nextval('customers_customer_id_seq') || '@example.com', 'New Customer', %s) "
            "RETURNING customer_id",
            (random.choice(COUNTRIES),),
        )
        return f"insert customer {cur.fetchone()[0]}"
    cur.execute(
        "UPDATE customers SET country = %s "
        "WHERE customer_id = (SELECT customer_id FROM customers ORDER BY random() LIMIT 1) RETURNING customer_id",
        (random.choice(COUNTRIES),),
    )
    return f"customer {cur.fetchone()[0]} moved country"


def delete_cancelled_order(cur) -> str:
    cur.execute(
        "DELETE FROM orders WHERE order_id = "
        "(SELECT order_id FROM orders WHERE status = 'cancelled' ORDER BY random() LIMIT 1) RETURNING order_id"
    )
    row = cur.fetchone()
    return f"delete order {row[0]} (+ items via cascade)" if row else advance_order(cur)


ACTIONS = [
    (new_order, 40),
    (advance_order, 35),
    (reprice_product, 10),
    (new_or_moved_customer, 10),
    (delete_cancelled_order, 5),
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-per-sec", type=float, default=2.0)
    parser.add_argument("--duration", type=int, default=0, help="seconds to run; 0 = until Ctrl+C")
    args = parser.parse_args()

    conninfo = (
        f"host={os.environ.get('SHOP_DB_HOST', 'localhost')} port={os.environ.get('SHOP_DB_PORT', '5433')} "
        f"dbname={os.environ.get('SHOP_DB_NAME', 'shop')} user={os.environ.get('SHOP_DB_USER', 'shop')} "
        f"password={os.environ.get('SHOP_DB_PASSWORD', 'shop')}"
    )
    funcs, weights = zip(*ACTIONS, strict=True)
    deadline = time.monotonic() + args.duration if args.duration else None

    with psycopg.connect(conninfo) as conn:
        try:
            while deadline is None or time.monotonic() < deadline:
                with conn.transaction(), conn.cursor() as cur:
                    print(random.choices(funcs, weights)[0](cur), flush=True)
                time.sleep(1 / args.events_per_sec)
        except KeyboardInterrupt:
            print("stopped")


if __name__ == "__main__":
    main()
