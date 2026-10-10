import argparse
import os
import random
import time

import psycopg2
from faker import Faker

fake = Faker()
DSN = os.getenv("DATABASE_URL", "postgresql://shop:shop_pw@localhost:5434/shop")
CATEGORIES = ["books", "toys", "kitchen", "garden", "electronics"]
NEXT_STATUS = {"placed": "paid", "paid": "shipped", "shipped": "delivered"}


def seed(cur, n_customers=50, n_products=20):
    for _ in range(n_customers):
        new_customer(cur)
    for _ in range(n_products):
        cur.execute(
            "INSERT INTO products (name, category, price) VALUES (%s, %s, %s)",
            (fake.word().title(), random.choice(CATEGORIES),
             round(random.uniform(5, 200), 2)),
        )


def new_customer(cur):
    cur.execute(
        "INSERT INTO customers (name, email, city) VALUES (%s, %s, %s)",
        (fake.name(), fake.unique.email(), fake.city()),
    )


def new_order(cur):
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    customer_id = cur.fetchone()[0]
    cur.execute("SELECT product_id, price FROM products ORDER BY random() LIMIT 1")
    product_id, price = cur.fetchone()
    cur.execute(
        "INSERT INTO orders (customer_id, product_id, quantity, unit_price) "
        "VALUES (%s, %s, %s, %s)",
        (customer_id, product_id, random.randint(1, 4), price),
    )


def advance_order(cur):
    cur.execute(
        "SELECT order_id, status FROM orders "
        "WHERE status IN ('placed','paid','shipped') ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            "UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s",
            (NEXT_STATUS[row[1]], row[0]),
        )


def change_price(cur):
    cur.execute(
        "UPDATE products SET price = round(price * %s, 2), updated_at = now() "
        "WHERE product_id = (SELECT product_id FROM products ORDER BY random() LIMIT 1)",
        (random.choice([0.9, 0.95, 1.05, 1.1]),),
    )


def change_address(cur):
    cur.execute(
        "UPDATE customers SET city = %s, updated_at = now() "
        "WHERE customer_id = (SELECT customer_id FROM customers ORDER BY random() LIMIT 1)",
        (fake.city(),),
    )


def delete_abandoned_order(cur):
    cur.execute(
        "DELETE FROM orders WHERE order_id = "
        "(SELECT order_id FROM orders WHERE status = 'placed' ORDER BY random() LIMIT 1)"
    )


ACTIONS = [
    (new_customer, 5), (new_order, 45), (advance_order, 25),
    (change_price, 5), (change_address, 8), (delete_abandoned_order, 7),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=200)
    parser.add_argument("--sleep", type=float, default=0.1)
    parser.add_argument("--seed-data", action="store_true")
    args = parser.parse_args()

    conn = psycopg2.connect(DSN)
    with conn, conn.cursor() as cur:      # `with conn` commits on success
        if args.seed_data:
            seed(cur)

    funcs, weights = zip(*ACTIONS)
    for i in range(args.events):
        action = random.choices(funcs, weights)[0]
        with conn, conn.cursor() as cur:  # one transaction per action
            action(cur)
        print(f"{i + 1:>4}  {action.__name__}")
        time.sleep(args.sleep)


if __name__ == "__main__":
    main()