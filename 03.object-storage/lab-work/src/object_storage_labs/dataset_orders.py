"""Generate reproducible orders linked to the generated users."""

import argparse
import datetime as dt
from typing import Any

from faker import Faker
from faker.providers import DynamicProvider

from .dataset_users import users_generate
from .serialize import serialize

PRODUCTS = ["bread", "brioche", "cookie", "croissant", "donut", "drink"]


def orders_generate(
    count_min: int = 0,
    count_max: int = 100,
    count_users: int = 50,
    date_from: dt.datetime | None = None,
    output: str = "",
) -> list[dict[str, Any]]:
    """Create deterministic orders, with each record linked by ``user_uuid``."""
    if count_min < 0 or count_max < 0 or count_users < 0:
        raise ValueError("counts must be non-negative")
    if count_min > count_max:
        raise ValueError("count_min cannot exceed count_max")

    fake = Faker()
    fake.seed_instance(42)
    fake.add_provider(DynamicProvider(provider_name="product", elements=PRODUCTS))
    date_start = date_from or dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
    if date_start.tzinfo is None:
        date_start = date_start.replace(tzinfo=dt.UTC)

    orders: list[dict[str, Any]] = []
    for user in users_generate(count_users):
        for _ in range(fake.pyint(min_value=count_min, max_value=count_max)):
            orders.append(
                {
                    "uuid": fake.uuid4(),
                    "user_uuid": user["uuid"],
                    "date": fake.date_time_between(
                        date_start, date_start + dt.timedelta(hours=1), tzinfo=dt.UTC
                    ),
                    "quantity": fake.pyint(min_value=1, max_value=5),
                    "product": fake.product(),
                }
            )
            date_start += dt.timedelta(hours=1)
    if output:
        serialize(orders, output)
    return orders


def parse_date(value: str) -> dt.datetime:
    """Parse an ISO timestamp and normalize naive values to UTC."""
    parsed = dt.datetime.fromisoformat(value)
    return parsed.replace(tzinfo=dt.UTC) if parsed.tzinfo is None else parsed


def main() -> None:
    parser = argparse.ArgumentParser(prog="dataset-orders", description="Orders generator")
    parser.add_argument("-C", "--count-min", type=int, default=0, help="Minimum orders per user.")
    parser.add_argument("-c", "--count-max", type=int, default=100, help="Maximum orders per user.")
    parser.add_argument("-d", "--date-from", type=parse_date, help="First order date in ISO format.")
    parser.add_argument(
        "-o", "--output", choices=["csv", "json", "jsonline"], default="json", help="Output format."
    )
    parser.add_argument("-u", "--count-users", type=int, default=50, help="Number of users.")
    args = parser.parse_args()
    orders_generate(args.count_min, args.count_max, args.count_users, args.date_from, args.output)


if __name__ == "__main__":
    main()
