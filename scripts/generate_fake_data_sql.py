import argparse
import random
from pathlib import Path


FIRST_NAMES = [
    "Alice",
    "Bob",
    "Charlie",
    "Diana",
    "Eve",
    "Frank",
    "Grace",
    "Heidi",
    "Ivan",
    "Judy",
    "Karl",
    "Leo",
    "Mallory",
    "Nina",
    "Oscar",
    "Peggy",
    "Quinn",
    "Ruth",
    "Sam",
    "Trudy",
]

LAST_NAMES = [
    "Anderson",
    "Brown",
    "Clark",
    "Davis",
    "Evans",
    "Foster",
    "Garcia",
    "Harris",
    "Irwin",
    "Johnson",
    "Klein",
    "Lopez",
    "Miller",
    "Nguyen",
    "Ortiz",
    "Patel",
    "Quincy",
    "Reed",
    "Smith",
    "Turner",
]

CITIES = [
    "Austin",
    "Boston",
    "Chicago",
    "Denver",
    "El Paso",
    "Fresno",
    "Houston",
    "Miami",
    "New York",
    "Seattle",
]

CATEGORIES = ["Books", "Electronics", "Games", "Home", "Office", "Sports"]

ORDER_NOTES = [
    "Standard",
    "Gift",
    "Express",
    "Fragile",
    "Backorder",
    "Priority",
]


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def batched(values: list[str], batch_size: int) -> list[list[str]]:
    return [values[i : i + batch_size] for i in range(0, len(values), batch_size)]


def build_customers(row_count: int, rng: random.Random) -> list[str]:
    rows = []
    for idx in range(1, row_count + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        full_name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{idx}@example.com"
        city = rng.choice(CITIES)
        rows.append(
            f"({idx}, {sql_string(full_name)}, {sql_string(email)}, {sql_string(city)})"
        )
    return rows


def build_products(row_count: int, rng: random.Random) -> list[str]:
    rows = []
    for idx in range(1, row_count + 1):
        category = rng.choice(CATEGORIES)
        name = f"{category} Item {idx}"
        price_cents = rng.randint(500, 25000)
        rows.append(
            f"({idx}, {sql_string(name)}, {sql_string(category)}, {price_cents})"
        )
    return rows


def build_orders(row_count: int, rng: random.Random) -> list[str]:
    rows = []
    for idx in range(1, row_count + 1):
        customer_id = rng.randint(1, row_count)
        product_id = rng.randint(1, row_count)
        quantity = rng.randint(1, 8)
        note = rng.choice(ORDER_NOTES)
        rows.append(
            f"({idx}, {customer_id}, {product_id}, {quantity}, {sql_string(note)})"
        )
    return rows


def build_sql(
    warehouse: str,
    database: str,
    schema: str,
    row_count: int,
    batch_size: int,
    seed: int,
) -> str:
    rng = random.Random(seed)

    customers = build_customers(row_count, rng)
    products = build_products(row_count, rng)
    orders = build_orders(row_count, rng)

    customers_dict = {
        "id": "INT",
        "name": "STRING",
        "email": "STRING",
        "city": "STRING",
    }
    
    products_dict = {
        "id": "INT",
        "name": "STRING",
        "category": "STRING",
        "price_cents": "INT",
    }
    orders_dict = {
        "id": "INT",
        "customer_id": "INT",
        "product_id": "INT",
        "quantity": "INT",
        "note": "STRING",
    }

    fields_customers = ", ".join(f"{col} {dtype}" for col, dtype in customers_dict.items())
    fields_products = ", ".join(f"{col} {dtype}" for col, dtype in products_dict.items())
    fields_orders = ", ".join(f"{col} {dtype}" for col, dtype in orders_dict.items())

    lines = [
        f"use warehouse {warehouse};",
        "",
        f"create database {database};",
        f"create schema {database}.{schema};",
        "",
        f"create table {database}.{schema}.customers ({fields_customers});",
        f"create table {database}.{schema}.products ({fields_products});",
        f"create table {database}.{schema}.orders ({fields_orders});",
        "",
        f"-- generated rows per table: {row_count}",
        f"-- insert batch size: {batch_size}",
        f"-- deterministic seed: {seed}",
        "",
    ]

    insert_fields_customers = ", ".join(customers_dict.keys())
    insert_fields_products = ", ".join(products_dict.keys())
    insert_fields_orders = ", ".join(orders_dict.keys())


    for chunk in batched(customers, batch_size):
        lines.append(
            f"insert into {database}.{schema}.customers ({insert_fields_customers}) values " + ", ".join(chunk) + ";"
        )

    lines.append("")

    for chunk in batched(products, batch_size):
        lines.append(
            f"insert into {database}.{schema}.products ({insert_fields_products}) values " + ", ".join(chunk) + ";"
        )

    lines.append("")

    for chunk in batched(orders, batch_size):
        lines.append(
            f"insert into {database}.{schema}.orders ({insert_fields_orders}) values " + ", ".join(chunk) + ";"
        )

    lines.extend(
        [
            "",
            f"select * from {database}.{schema}.customers order by id limit 5;",
            f"select * from {database}.{schema}.products order by id limit 5;",
            f"select * from {database}.{schema}.orders order by id limit 5;",
        ]
    )

    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate client-ready SQL with fake data for Kionas tables."
    )
    parser.add_argument(
        "--output",
        default="scripts/generated_fake_data.sql",
        help="Output SQL file path.",
    )
    parser.add_argument(
        "--warehouse",
        default="kionas-worker1",
        help="Warehouse to use in the generated SQL.",
    )
    parser.add_argument(
        "--database",
        default="bench",
        help="Database name for generated objects.",
    )
    parser.add_argument(
        "--schema",
        default="seed1",
        help="Schema name for generated objects.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100000,
        help="Rows to generate per table.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="Number of rows per INSERT statement.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Deterministic random seed.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.rows <= 0:
        raise SystemExit("--rows must be greater than 0")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be greater than 0")

    sql = build_sql(
        warehouse=args.warehouse,
        database=args.database,
        schema=args.schema,
        row_count=args.rows,
        batch_size=args.batch_size,
        seed=args.seed,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(sql, encoding="utf-8")

    print(f"Generated SQL file: {output_path}")
    print(
        f"Tables: {args.database}.{args.schema}.customers/products/orders with {args.rows} rows each"
    )


if __name__ == "__main__":
    main()
