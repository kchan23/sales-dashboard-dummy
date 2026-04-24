import sys
import types
import unittest
from unittest.mock import patch


fake_bigquery_module = types.ModuleType("database.bigquery")
fake_bigquery_module.BigQueryManager = object
sys.modules.setdefault("database.bigquery", fake_bigquery_module)

from integrations.toast_api.scheduler import compute_date_range, pull_restaurant, to_api_date


class FixedDateTime:
    @classmethod
    def now(cls):
        from datetime import datetime

        return datetime(2026, 4, 11, 12, 0, 0)

    @classmethod
    def strptime(cls, value, fmt):
        from datetime import datetime

        return datetime.strptime(value, fmt)


class FakeBigQuery:
    def __init__(self, latest=None):
        self.latest = latest
        self.stream_calls = []
        self.log_calls = []

    def get_latest_import_date(self, location_id, source=None):
        return self.latest

    def stream_rows(self, table, rows):
        self.stream_calls.append((table, rows))
        return len(rows)

    def log_import(self, location_id, import_date, source, table_name, row_count):
        self.log_calls.append((location_id, import_date, source, table_name, row_count))


class FakeClient:
    def __init__(self, orders=None, menus=None, fail_orders=False, fail_menus=False):
        self.orders = orders if orders is not None else []
        self.menus = menus if menus is not None else []
        self.fail_orders = fail_orders
        self.fail_menus = fail_menus
        self.restaurant_ids = []

    def set_restaurant(self, restaurant_guid):
        self.restaurant_ids.append(restaurant_guid)

    def get_orders_bulk(self, start_date, end_date):
        if self.fail_orders:
            raise RuntimeError("orders failed")
        return self.orders

    def get_menus(self):
        if self.fail_menus:
            raise RuntimeError("menus failed")
        return self.menus


class SchedulerTests(unittest.TestCase):
    @patch("integrations.toast_api.scheduler.datetime", FixedDateTime)
    def test_compute_date_range_uses_last_import_plus_one_day(self):
        bq = FakeBigQuery(latest="20260408")

        date_range = compute_date_range(bq, "loc-1", interval_days=30)

        self.assertEqual(("20260409", "20260410"), date_range)

    @patch("integrations.toast_api.scheduler.datetime", FixedDateTime)
    def test_compute_date_range_returns_none_when_up_to_date(self):
        bq = FakeBigQuery(latest="20260410")

        date_range = compute_date_range(bq, "loc-1", interval_days=30)

        self.assertIsNone(date_range)

    @patch("integrations.toast_api.scheduler.datetime", FixedDateTime)
    def test_compute_date_range_uses_interval_when_no_history(self):
        bq = FakeBigQuery(latest=None)

        date_range = compute_date_range(bq, "loc-1", interval_days=7)

        self.assertEqual(("20260404", "20260410"), date_range)

    def test_to_api_date_formats_yyyymmdd(self):
        self.assertEqual("2026-04-10", to_api_date("20260410"))

    @patch("integrations.toast_api.scheduler._update_location_name_cache")
    @patch("integrations.toast_api.scheduler.compute_date_range", return_value=("20260401", "20260402"))
    def test_pull_restaurant_streams_all_datasets(self, _compute_date_range, update_cache):
        client = FakeClient(
            orders=[
                {
                    "guid": "order-1",
                    "businessDate": 20260401,
                    "openedDate": "2026-04-01T10:00:00Z",
                    "source": "IN_STORE",
                    "checks": [
                        {
                            "payments": [{"type": "CARD", "amount": 10.0, "tipAmount": 1.0}],
                            "selections": [{"displayName": "Noodles", "quantity": 1, "price": 10.0}],
                            "customer": {"email": "guest@example.com"},
                            "totalAmount": 11.0,
                            "amount": 10.0,
                            "taxAmount": 1.0,
                            "appliedDiscounts": [],
                        }
                    ],
                }
            ],
            menus=[{"menus": [{"menuGroups": [{"name": "Entrees", "menuItems": [{"name": "Noodles", "price": 10.0}]}]}]}],
        )
        bq = FakeBigQuery()

        stats = pull_restaurant(
            client=client,
            bq=bq,
            restaurant_guid="loc-1",
            restaurant_name="Downtown",
            interval_days=30,
        )

        self.assertEqual("success", stats["status"])
        self.assertEqual(1, stats["orders"])
        self.assertEqual(1, stats["order_items"])
        self.assertEqual(1, stats["payments"])
        self.assertEqual(1, stats["customer_orders"])
        self.assertEqual(1, stats["menus"])
        self.assertEqual(["loc-1"], client.restaurant_ids)
        self.assertEqual(["orders", "order_items", "payments", "customer_orders", "inventory"], [call[0] for call in bq.stream_calls])
        self.assertEqual(2, len(bq.log_calls))
        update_cache.assert_called_once_with("loc-1", "Downtown")

    @patch("integrations.toast_api.scheduler._update_location_name_cache")
    @patch("integrations.toast_api.scheduler.compute_date_range", return_value=("20260401", "20260402"))
    def test_pull_restaurant_customer_only_skips_non_customer_tables(self, _compute_date_range, update_cache):
        client = FakeClient(
            orders=[
                {
                    "guid": "order-1",
                    "businessDate": 20260401,
                    "checks": [{"customer": {"email": "guest@example.com"}}],
                }
            ]
        )
        bq = FakeBigQuery()

        stats = pull_restaurant(
            client=client,
            bq=bq,
            restaurant_guid="loc-1",
            restaurant_name="Downtown",
            interval_days=30,
            customer_only=True,
        )

        self.assertEqual("success", stats["status"])
        self.assertEqual(1, stats["customer_orders"])
        self.assertEqual(0, stats["orders"])
        self.assertEqual(["customer_orders"], [call[0] for call in bq.stream_calls])
        update_cache.assert_called_once_with("loc-1", "Downtown")


if __name__ == "__main__":
    unittest.main()
