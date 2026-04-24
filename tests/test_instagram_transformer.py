import json
import unittest
from datetime import datetime, timezone

from integrations.instagram_api.transformer import (
    transform_media_snapshots,
    transform_profile_snapshot,
)


class InstagramTransformerTests(unittest.TestCase):
    def test_transform_profile_snapshot_maps_fields_and_formats_snapshot(self):
        row = transform_profile_snapshot(
            {
                "id": "acct-1",
                "username": "doughzone",
                "name": "Dough Zone",
                "biography": " Dumplings ",
                "account_type": "BUSINESS",
                "media_count": "42",
                "followers_count": "1000",
                "follows_count": 12,
                "profile_picture_url": "https://example.test/pic.jpg",
            },
            "Main Brand",
            "run-123",
            datetime(2026, 4, 21, 15, 45, tzinfo=timezone.utc),
            local_timezone="America/Los_Angeles",
        )

        self.assertEqual("acct-1", row["account_id"])
        self.assertEqual("Main Brand", row["account_label"])
        self.assertEqual("doughzone", row["username"])
        self.assertEqual("Dough Zone", row["name"])
        self.assertEqual("Dumplings", row["biography"])
        self.assertEqual(42, row["media_count"])
        self.assertEqual(1000, row["followers_count"])
        self.assertEqual(12, row["follows_count"])
        self.assertEqual("2026-04-21T15:45:00+00:00", row["snapshot_at"])
        self.assertEqual("20260421", row["snapshot_date"])
        self.assertEqual("run-123", row["source_run_id"])
        self.assertEqual("America/Los_Angeles", row["local_timezone"])

    def test_transform_media_snapshots_serializes_children_and_keeps_missing_metrics_null(self):
        rows = transform_media_snapshots(
            {"id": "acct-1", "username": "doughzone"},
            [
                {
                    "id": "media-1",
                    "caption": "Spring launch",
                    "media_type": "CAROUSEL_ALBUM",
                    "media_product_type": "FEED",
                    "permalink": "https://instagram.test/p/1",
                    "media_url": "https://cdn.test/cover.jpg",
                    "thumbnail_url": "https://cdn.test/thumb.jpg",
                    "timestamp": "2026-04-20T20:30:00Z",
                    "like_count": "15",
                    "comments_count": 4,
                    "reach": "120",
                    "children": {
                        "data": [
                            {
                                "id": "child-1",
                                "media_type": "IMAGE",
                                "media_url": "https://cdn.test/child-1.jpg",
                                "thumbnail_url": "https://cdn.test/child-1-thumb.jpg",
                                "timestamp": "2026-04-20T20:30:00+0000",
                            }
                        ]
                    },
                }
            ],
            "Main Brand",
            "run-456",
        )

        self.assertEqual(1, len(rows))
        row = rows[0]
        self.assertEqual("acct-1", row["account_id"])
        self.assertEqual("Main Brand", row["account_label"])
        self.assertEqual("doughzone", row["username"])
        self.assertEqual("media-1", row["media_id"])
        self.assertEqual("CAROUSEL", row["media_type"])
        self.assertEqual("20260420", row["posted_date_utc"])
        self.assertEqual("2026-04-20T20:30:00+00:00", row["posted_at_utc"])
        self.assertEqual(15, row["likes"])
        self.assertEqual(4, row["comments_count"])
        self.assertEqual(120, row["reach"])
        self.assertIsNone(row["views"])
        self.assertIsNone(row["saved"])
        self.assertIsNone(row["shares"])
        self.assertIsNone(row["total_interactions"])
        self.assertEqual(1, row["child_count"])
        self.assertEqual("run-456", row["source_run_id"])

        children = json.loads(row["children_json"])
        self.assertEqual(
            {
                "id": "child-1",
                "media_type": "IMAGE",
                "timestamp": "2026-04-20T20:30:00+0000",
                "posted_at_utc": "2026-04-20T20:30:00+00:00",
                "posted_date_utc": "20260420",
                "media_url": "https://cdn.test/child-1.jpg",
                "thumbnail_url": "https://cdn.test/child-1-thumb.jpg",
            },
            children[0],
        )


if __name__ == "__main__":
    unittest.main()
