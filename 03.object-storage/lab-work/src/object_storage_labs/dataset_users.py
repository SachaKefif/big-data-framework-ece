"""Generate a reproducible dataset of users."""

import argparse
from typing import Any

from faker import Faker

from .serialize import serialize


def users_generate(count: int = 50, output: str = "") -> list[dict[str, Any]]:
    """Create ``count`` deterministic user records and optionally serialize them."""
    if count < 0:
        raise ValueError("count must be non-negative")

    fake = Faker()
    fake.seed_instance(42)
    users = [{"uuid": fake.uuid4(), **fake.simple_profile()} for _ in range(count)]
    if output:
        serialize(users, output)
    return users


def main() -> None:
    parser = argparse.ArgumentParser(prog="dataset-users", description="Users generator")
    parser.add_argument("-c", "--count", type=int, default=50, help="Number of users to generate.")
    parser.add_argument(
        "-o", "--output", choices=["csv", "json", "jsonline"], default="json", help="Output format."
    )
    args = parser.parse_args()
    users_generate(args.count, args.output)


if __name__ == "__main__":
    main()
