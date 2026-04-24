import sys
import types
import unittest


fake_bigquery_module = types.ModuleType("database.bigquery")
fake_bigquery_module.BigQueryManager = object
sys.modules.setdefault("database.bigquery", fake_bigquery_module)

from database.create_views import VIEWS


class InstagramViewTests(unittest.TestCase):
    def test_instagram_profiles_current_view_uses_latest_snapshot_and_timestamp_parsing(self):
        sql = VIEWS["instagram_profiles_current"]

        self.assertIn("PARTITION BY account_id", sql)
        self.assertIn("ORDER BY created_at DESC", sql)
        self.assertIn("SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', snapshot_at)", sql)
        self.assertIn("SAFE.PARSE_DATE('%Y%m%d', snapshot_date) AS snapshot_date", sql)

    def test_instagram_media_current_view_exposes_current_state_fields(self):
        sql = VIEWS["instagram_media_current"]

        self.assertIn("PARTITION BY account_id, media_id", sql)
        self.assertIn("SAFE.PARSE_DATE('%Y%m%d', posted_date_utc) AS posted_date", sql)
        self.assertIn(
            "COALESCE(total_interactions, COALESCE(likes, 0) + COALESCE(comments_count, 0)) AS engagement_total",
            sql,
        )
        self.assertIn("UPPER(COALESCE(media_type, '')) = 'CAROUSEL' AS is_carousel", sql)


if __name__ == "__main__":
    unittest.main()
