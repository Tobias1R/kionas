import argparse
import datetime as dt
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

PAYMENT_METHODS = ["card", "bank_transfer", "wallet", "cash"]
PAYMENT_STATUS = ["authorized", "captured", "failed", "refunded"]
SHIPMENT_CARRIERS = ["DHL", "FedEx", "UPS", "USPS", "GLS"]
SHIPMENT_STATUS = ["processing", "in_transit", "delivered", "returned"]


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def batched(values: list[str], batch_size: int) -> list[list[str]]:
    return [values[i : i + batch_size] for i in range(0, len(values), batch_size)]


def build_customers(row_count: int, initial_idx: int, rng: random.Random) -> list[str]:
    start_date = dt.date(1950, 1, 1)
    end_date = dt.date(2005, 12, 31)
    days_range = (end_date - start_date).days

    rows = []
    end_idx = initial_idx + row_count
    for idx in range(initial_idx, end_idx):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        full_name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{idx}@example.com"
        city = rng.choice(CITIES)
        birthday = start_date + dt.timedelta(days=rng.randint(0, days_range))
        rows.append(
            f"({idx}, {sql_string(full_name)}, {sql_string(email)}, {sql_string(city)}, {sql_string(birthday.isoformat())})"
        )
    return rows


def build_products(row_count: int, initial_idx: int, rng: random.Random) -> list[str]:
    rows = []
    end_idx = initial_idx + row_count
    for idx in range(initial_idx, end_idx):
        category = rng.choice(CATEGORIES)
        name = f"{category} Item {idx}"
        price_cents = rng.randint(500, 25000)
        rows.append(
            f"({idx}, {sql_string(name)}, {sql_string(category)}, {price_cents})"
        )
    return rows


def build_orders(row_count: int, initial_idx: int, rng: random.Random) -> list[str]:
    start_dt = dt.datetime(2021, 1, 1, 0, 0, 0)
    end_dt = dt.datetime(2025, 12, 31, 23, 59, 59)
    seconds_range = int((end_dt - start_dt).total_seconds())

    rows = []
    order_count = row_count * 2
    end_idx = initial_idx + order_count
    max_dim_id = initial_idx + row_count - 1
    for idx in range(initial_idx, end_idx):
        customer_id = rng.randint(initial_idx, max_dim_id)
        product_id = rng.randint(initial_idx, max_dim_id)
        quantity = rng.randint(1, 8)
        note = rng.choice(ORDER_NOTES)
        ordered_at = start_dt + dt.timedelta(seconds=rng.randint(0, seconds_range))
        rows.append(
            f"({idx}, {customer_id}, {product_id}, {quantity}, {sql_string(note)}, {sql_string(ordered_at.strftime('%Y-%m-%d %H:%M:%S'))})"
        )
    return rows


def build_payments(
    order_count: int,
    row_count: int,
    initial_idx: int,
    rng: random.Random,
) -> list[str]:
    rows = []
    end_idx = initial_idx + order_count
    max_customer_id = initial_idx + row_count - 1
    for idx in range(initial_idx, end_idx):
        order_id = idx
        customer_id = rng.randint(initial_idx, max_customer_id)
        method = rng.choice(PAYMENT_METHODS)
        status = rng.choices(PAYMENT_STATUS, weights=[20, 65, 10, 5], k=1)[0]
        amount_cents = rng.randint(800, 200000)
        rows.append(
            f"({idx}, {order_id}, {customer_id}, {sql_string(method)}, {sql_string(status)}, {amount_cents})"
        )
    return rows


def build_shipments(order_count: int, initial_idx: int, rng: random.Random) -> list[str]:
    rows = []
    end_idx = initial_idx + order_count
    for idx in range(initial_idx, end_idx):
        order_id = idx
        carrier = rng.choice(SHIPMENT_CARRIERS)
        status = rng.choices(SHIPMENT_STATUS, weights=[15, 35, 45, 5], k=1)[0]
        tracking_code = f"TRK-{order_id:08d}-{rng.randint(1000, 9999)}"
        rows.append(
            f"({idx}, {order_id}, {sql_string(carrier)}, {sql_string(status)}, {sql_string(tracking_code)})"
        )
    return rows


def build_sql(
    warehouse: str,
    database: str,
    schema: str,
    row_count: int,
    batch_size: int,
    seed: int,
    initial_idx: int,
) -> str:
    rng = random.Random(seed)

    customers = build_customers(row_count, initial_idx, rng)
    products = build_products(row_count, initial_idx, rng)
    orders = build_orders(row_count, initial_idx, rng)
    order_count = row_count * 2
    payments = build_payments(order_count, row_count, initial_idx, rng)
    shipments = build_shipments(order_count, initial_idx, rng)

    customers_dict = {
        "id": "INT",
        "name": "STRING",
        "email": "STRING",
        "city": "STRING",
        "birthday": "DATE",
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
        "ordered_at": "TIMESTAMP",
    }

    payments_dict = {
        "id": "INT",
        "order_id": "INT",
        "customer_id": "INT",
        "method": "STRING",
        "status": "STRING",
        "amount_cents": "INT",
    }

    shipments_dict = {
        "id": "INT",
        "order_id": "INT",
        "carrier": "STRING",
        "status": "STRING",
        "tracking_code": "STRING",
    }

    fields_customers = ", ".join(f"{col} {dtype}" for col, dtype in customers_dict.items())
    fields_products = ", ".join(f"{col} {dtype}" for col, dtype in products_dict.items())
    fields_orders = ", ".join(f"{col} {dtype}" for col, dtype in orders_dict.items())
    fields_payments = ", ".join(
        f"{col} {dtype}" for col, dtype in payments_dict.items()
    )
    fields_shipments = ", ".join(
        f"{col} {dtype}" for col, dtype in shipments_dict.items()
    )

    lines = [
        f"use warehouse {warehouse};",
        "",
        f"create database {database};",
        f"create schema {database}.{schema};",
        "",
        f"create table {database}.{schema}.customers ({fields_customers});",
        f"create table {database}.{schema}.products ({fields_products});",
        f"create table {database}.{schema}.orders ({fields_orders});",
        f"create table {database}.{schema}.payments ({fields_payments});",
        f"create table {database}.{schema}.shipments ({fields_shipments});",
        "",
        f"-- generated rows per table: {row_count}",
        f"-- generated rows orders/payments/shipments: {order_count}",
        f"-- id range start index: {initial_idx}",
        f"-- insert batch size: {batch_size}",
        f"-- deterministic seed: {seed}",
        "",
    ]

    insert_fields_customers = ", ".join(customers_dict.keys())
    insert_fields_products = ", ".join(products_dict.keys())
    insert_fields_orders = ", ".join(orders_dict.keys())
    insert_fields_payments = ", ".join(payments_dict.keys())
    insert_fields_shipments = ", ".join(shipments_dict.keys())


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

    lines.append("")

    for chunk in batched(payments, batch_size):
        lines.append(
            f"insert into {database}.{schema}.payments ({insert_fields_payments}) values "
            + ", ".join(chunk)
            + ";"
        )

    lines.append("")

    for chunk in batched(shipments, batch_size):
        lines.append(
            f"insert into {database}.{schema}.shipments ({insert_fields_shipments}) values "
            + ", ".join(chunk)
            + ";"
        )

    lines.extend(
        [
            "",
            f"select * from {database}.{schema}.customers order by id limit 5;",
            f"select * from {database}.{schema}.products order by id limit 5;",
            f"select * from {database}.{schema}.orders order by id limit 5;",
            f"select * from {database}.{schema}.payments order by id limit 5;",
            f"select * from {database}.{schema}.shipments order by id limit 5;",
            "",
            "-- benchmark join sample 1: customer x order x product x payment x shipment",
            f"select c.city, p.category, pay.method, ship.carrier, count(*) as order_count "
            f"from {database}.{schema}.customers c "
            f"join {database}.{schema}.orders o on c.id = o.customer_id "
            f"join {database}.{schema}.products p on p.id = o.product_id "
            f"join {database}.{schema}.payments pay on pay.order_id = o.id "
            f"join {database}.{schema}.shipments ship on ship.order_id = o.id "
            "group by c.city, p.category, pay.method, ship.carrier "
            "order by order_count desc limit 20;",
            "",
            "-- benchmark join sample 2: age bucket x payment status x shipment status",
            f"select case "
            "when date_part('year', age(cast(current_date as timestamp), cast(c.birthday as timestamp))) < 30 then 'under_30' "
            "when date_part('year', age(cast(current_date as timestamp), cast(c.birthday as timestamp))) < 45 then '30_44' "
            "when date_part('year', age(cast(current_date as timestamp), cast(c.birthday as timestamp))) < 60 then '45_59' "
            "else '60_plus' end as age_bucket, "
            "pay.status as payment_status, ship.status as shipment_status, "
            "sum(pay.amount_cents) as total_amount_cents "
            f"from {database}.{schema}.customers c "
            f"join {database}.{schema}.orders o on c.id = o.customer_id "
            f"join {database}.{schema}.payments pay on pay.order_id = o.id "
            f"join {database}.{schema}.shipments ship on ship.order_id = o.id "
            "group by age_bucket, pay.status, ship.status "
            "order by total_amount_cents desc limit 20;",
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
    parser.add_argument(
        "--initial-idx",
        type=int,
        default=1,
        help="Starting id for all generated primary/foreign key ranges.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.rows <= 0:
        raise SystemExit("--rows must be greater than 0")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be greater than 0")
    if args.initial_idx <= 0:
        raise SystemExit("--initial-idx must be greater than 0")

    sql = build_sql(
        warehouse=args.warehouse,
        database=args.database,
        schema=args.schema,
        row_count=args.rows,
        batch_size=args.batch_size,
        seed=args.seed,
        initial_idx=args.initial_idx,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(sql, encoding="utf-8")

    print(f"Generated SQL file: {output_path}")
    print(
        f"Tables: {args.database}.{args.schema}.customers/products ({args.rows} rows each), "
        f"orders/payments/shipments ({args.rows * 2} rows each)"
    )


if __name__ == "__main__":
    main()

# sample usage:
# python scripts/generate_fake_data_sql.py --output scripts/generated_fake_data_v2.sql --warehouse compute_small --database bench2 --schema seed1 --rows 1000000 --batch-size 2000 --seed 42