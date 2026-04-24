import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch


fake_bigquery_module = types.ModuleType("database.bigquery")
fake_bigquery_module.BigQueryManager = object
sys.modules.setdefault("database.bigquery", fake_bigquery_module)

from integrations.instagram_api.client import Config
from integrations.instagram_api.scheduler import run_sync


class FakeBigQuery:
    def __init__(self):
        self.create_schema_called = False
        self.stream_calls = []
        self.log_calls = []

    def create_schema(self):
        self.create_schema_called = True

    def stream_rows(self, table, rows):
        self.stream_calls.append((table, rows))
        return len(rows)

    def log_import(self, location_id, business_date, file_type, file_name, rows_imported):
        self.log_calls.append((location_id, business_date, file_type, file_name, rows_imported))


class FakeClient:
    def __init__(self):
        self.config = Config(
            access_token="token",
            api_version="v25.0",
            graph_host="graph.instagram.com",
            timeout=30,
            max_retries=3,
            retry_backoff_seconds=1,
            fetch_insights=False,
            media_limit=100,
            local_timezone_name="America/Los_Angeles",
            api_delay_seconds=0,
        )


class InstagramSchedulerTests(unittest.TestCase):
    @patch("integrations.instagram_api.scheduler.BigQueryManager", side_effect=AssertionError("should not initialize BigQuery"))
    @patch("integrations.instagram_api.scheduler.uuid.uuid4", return_value="run-dry")
    @patch("integrations.instagram_api.scheduler.get_media", return_value=[{"id": "media-1", "timestamp": "2026-04-20T18:00:00Z"}])
    @patch("integrations.instagram_api.scheduler.get_profile", return_value={"id": "acct-1", "username": "doughzone"})
    def test_run_sync_dry_run_fetches_without_bigquery(
        self,
        _get_profile,
        _get_media,
        _uuid4,
        _bq_manager,
    ):
        stats = run_sync(
            dry_run=True,
            client=FakeClient(),
            now=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual("dry_run", stats["status"])
        self.assertEqual("acct-1", stats["account_id"])
        self.assertEqual("2026-03-22", stats["window_start"])
        self.assertEqual("2026-04-21", stats["window_end"])
        self.assertEqual(1, stats["profile_rows"])
        self.assertEqual(1, stats["media_rows"])
        self.assertEqual("run-dry", stats["run_id"])

    @patch("integrations.instagram_api.scheduler.uuid.uuid4", return_value="run-live")
    @patch("integrations.instagram_api.scheduler.get_profile", return_value={"id": "acct-1", "username": "doughzone"})
    def test_run_sync_live_writes_tables_and_logs_imports(self, _get_profile, uuid4_mock):
        bq = FakeBigQuery()
        client = FakeClient()

        with patch(
            "integrations.instagram_api.scheduler.get_media",
            return_value=[
                {
                    "id": "media-1",
                    "timestamp": "2026-04-20T18:00:00Z",
                    "media_type": "IMAGE",
                    "media_product_type": "FEED",
                }
            ],
        ) as get_media_mock:
            stats = run_sync(
                account_label="Main Brand",
                refresh_days=30,
                bq=bq,
                client=client,
                now=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
            )

        self.assertEqual("success", stats["status"])
        self.assertTrue(bq.create_schema_called)
        self.assertEqual(
            ["instagram_profile_snapshots", "instagram_media_snapshots"],
            [call[0] for call in bq.stream_calls],
        )
        self.assertEqual(2, len(bq.log_calls))
        self.assertEqual(
            ["INSTAGRAM_API_PROFILE", "INSTAGRAM_API_MEDIA"],
            [call[2] for call in bq.log_calls],
        )
        self.assertEqual(
            ("2026-03-22", "2026-04-21"),
            (get_media_mock.call_args.kwargs["start_date"], get_media_mock.call_args.kwargs["end_date"]),
        )
        self.assertEqual("run-live", stats["run_id"])
        self.assertEqual(1, stats["profile_rows"])
        self.assertEqual(1, stats["media_rows"])
        self.assertEqual("Main Brand", bq.stream_calls[0][1][0]["account_label"])
        uuid4_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
