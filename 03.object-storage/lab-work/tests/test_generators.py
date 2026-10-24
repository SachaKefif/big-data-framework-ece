"""Integration checks for bronze data consumed by all later labs."""

import csv
import io
import unittest
from contextlib import redirect_stdout

from object_storage_labs.dataset_orders import orders_generate
from object_storage_labs.dataset_users import users_generate


class GeneratorTests(unittest.TestCase):
    def test_orders_reference_users_and_have_unique_ids(self) -> None:
        users = users_generate()
        orders = orders_generate()
        user_ids = {user["uuid"] for user in users}
        order_ids = [order["uuid"] for order in orders]
        self.assertEqual(len(user_ids), 50)
        self.assertEqual(len(order_ids), len(set(order_ids)))
        self.assertTrue(orders)
        self.assertTrue(all(order["user_uuid"] in user_ids for order in orders))
        self.assertTrue(all(1 <= order["quantity"] <= 5 for order in orders))

    def test_multiline_address_survives_csv_round_trip(self) -> None:
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            users_generate(count=50, output="csv")
        rows = list(csv.DictReader(io.StringIO(buffer.getvalue())))
        self.assertEqual(len(rows), 50)
        self.assertTrue(all("\n" in row["address"] for row in rows))


if __name__ == "__main__":
    unittest.main()
