import sys
import types
import unittest
from types import SimpleNamespace


fake_bigquery = types.ModuleType("bigquery")
fake_bigquery.Client = object


class FakeQueryJobConfig:
    def __init__(self):
        self.query_parameters = None
        self.dry_run = False
        self.use_query_cache = True


fake_bigquery.QueryJobConfig = FakeQueryJobConfig
fake_bigquery.ScalarQueryParameter = object

google_module = types.ModuleType("google")
cloud_module = types.ModuleType("google.cloud")
cloud_module.bigquery = fake_bigquery
google_module.cloud = cloud_module

sys.modules.setdefault("google", google_module)
sys.modules.setdefault("google.cloud", cloud_module)
sys.modules.setdefault("google.cloud.bigquery", fake_bigquery)

from query.validator import SQLValidator, validate_table_references


class FakeQueryJob:
    total_bytes_processed = 123


class FakeBigQueryClient:
    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def query(self, sql, job_config=None):
        self.calls.append((sql, job_config))
        if self.error:
            raise self.error
        return FakeQueryJob()


class SQLValidatorTests(unittest.TestCase):
    def make_params(self, *names):
        return [SimpleNamespace(name=name) for name in names]

    def test_validate_rejects_forbidden_keywords(self):
        validator = SQLValidator(FakeBigQueryClient())

        valid, error = validator.validate("DELETE FROM orders", [])

        self.assertFalse(valid)
        self.assertIn("DELETE", error)

    def test_validate_rejects_non_select_queries(self):
        validator = SQLValidator(FakeBigQueryClient())

        valid, error = validator.validate("WITH x AS (SELECT 1) SELECT * FROM x", [])

        self.assertFalse(valid)
        self.assertEqual("Only SELECT queries are permitted for analytics", error)

    def test_validate_rejects_pii_tables(self):
        validator = SQLValidator(FakeBigQueryClient())

        valid, error = validator.validate(
            "SELECT * FROM customer_orders WHERE business_date >= @start_date",
            self.make_params("start_date"),
        )

        self.assertFalse(valid)
        self.assertIn("Privacy violation", error)

    def test_validate_rejects_raw_order_identifier_output(self):
        validator = SQLValidator(FakeBigQueryClient())

        valid, error = validator.validate(
            "SELECT order_guid, total_amount FROM orders WHERE location_id = @location_id",
            self.make_params("location_id"),
        )

        self.assertFalse(valid)
        self.assertIn("order_guid cannot appear as a raw output column", error)

    def test_validate_allows_aggregated_identifier_output(self):
        client = FakeBigQueryClient()
        validator = SQLValidator(client)

        valid, error = validator.validate(
            """
            SELECT COUNT(DISTINCT order_guid) AS orders
            FROM orders
            WHERE location_id = @location_id
              AND business_date BETWEEN @start_date AND @end_date
            """,
            self.make_params("location_id", "start_date", "end_date"),
        )

        self.assertTrue(valid)
        self.assertIsNone(error)
        self.assertEqual(1, len(client.calls))

    def test_validate_reports_missing_parameters(self):
        validator = SQLValidator(FakeBigQueryClient())

        valid, error = validator.validate(
            "SELECT * FROM orders WHERE location_id = @location_id AND business_date >= @start_date",
            self.make_params("location_id"),
        )

        self.assertFalse(valid)
        self.assertIn("start_date", error)

    def test_dry_run_surfaces_syntax_errors(self):
        validator = SQLValidator(FakeBigQueryClient(error=Exception("Syntax error: unexpected keyword")))

        valid, error = validator._dry_run("SELECT * FROM orders", [])

        self.assertFalse(valid)
        self.assertIn("SQL syntax error", error)

    def test_validate_table_references_rejects_unapproved_tables(self):
        valid, error = validate_table_references(
            "SELECT * FROM orders JOIN secret_table ON orders.id = secret_table.id",
            ["orders"],
        )

        self.assertFalse(valid)
        self.assertIn("SECRET_TABLE", error)


if __name__ == "__main__":
    unittest.main()
