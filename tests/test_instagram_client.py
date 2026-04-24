import unittest

from integrations.instagram_api.client import Config, get_media


class FakeClient:
    def __init__(self, items):
        self.config = Config(
            access_token="token",
            api_version="v25.0",
            graph_host="graph.instagram.com",
            timeout=30,
            max_retries=3,
            retry_backoff_seconds=1,
            fetch_insights=False,
            media_limit=100,
            local_timezone_name="",
            api_delay_seconds=0,
        )
        self.items = list(items)
        self.seen_ids = []
        self.paginate_calls = []

    def paginate(self, path_or_url, *, params=None, include_access_token=True):
        self.paginate_calls.append((path_or_url, dict(params or {}), include_access_token))
        for item in self.items:
            self.seen_ids.append(item["id"])
            yield item


class InstagramClientTests(unittest.TestCase):
    def test_get_media_stops_once_post_is_older_than_start_date(self):
        client = FakeClient(
            [
                {"id": "a", "timestamp": "2026-04-10T12:00:00Z"},
                {"id": "b", "timestamp": "2026-04-05T12:00:00Z"},
                {"id": "c", "timestamp": "2026-04-01T00:00:00Z"},
                {"id": "d", "timestamp": "2026-03-31T23:59:59Z"},
                {"id": "e", "timestamp": "2026-03-20T12:00:00Z"},
            ]
        )

        media = get_media(
            client,
            start_date="2026-04-01",
            end_date="2026-04-10",
            media_limit=25,
        )

        self.assertEqual(["a", "b", "c"], [item["id"] for item in media])
        self.assertEqual(["a", "b", "c", "d"], client.seen_ids)
        self.assertEqual("/me/media", client.paginate_calls[0][0])
        self.assertEqual(25, client.paginate_calls[0][1]["limit"])


if __name__ == "__main__":
    unittest.main()
