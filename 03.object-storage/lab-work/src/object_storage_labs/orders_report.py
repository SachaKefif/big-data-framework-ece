"""Monthly DuckDB report over the bronze orders dataset."""

import argparse
import os

import duckdb


def orders_report(product: str | None = None, bronze_root: str | None = None) -> list[tuple]:
    """Return monthly order counts and quantities, optionally for one product.

    ``LAB_BRONZE_ROOT`` supports local verification. On Onyxia the default is
    the same S3 bronze location used throughout the course.
    """
    if bronze_root is None:
        bronze_root = os.environ.get("LAB_BRONZE_ROOT")
    if bronze_root is None:
        bronze_root = f"s3://{os.environ['LAB_BUCKET_NAME']}/bronze"

    with duckdb.connect() as con:
        con.execute("SET TimeZone = 'UTC'")
        return con.execute(
            """
            SELECT strftime(date, '%Y-%m') AS month,
                   count(*) AS orders,
                   sum(quantity) AS quantity
            FROM read_csv($path, strict_mode = false)
            WHERE $product IS NULL OR product = $product
            GROUP BY month
            ORDER BY month
            """,
            {"path": f"{bronze_root.rstrip('/')}/orders.csv", "product": product},
        ).fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(prog="orders-report", description="Monthly orders report")
    parser.add_argument("-p", "--product", help="Filter orders by product")
    args = parser.parse_args()
    for month, orders, quantity in orders_report(args.product):
        print(f"{month}\t{orders}\t{quantity}")


if __name__ == "__main__":
    main()
