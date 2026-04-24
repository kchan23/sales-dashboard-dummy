import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from integrations.toast_api.client import ToastAPIClient


class FakeResponse:
    def __init__(self, json_data=None, status_code=200, headers=None):
        self._json_data = json_data
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400 and self.status_code != 429:
            raise RuntimeError(f"HTTP {self.status_code}")


class ToastAPIClientTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.credentials_path = Path(self.temp_dir.name) / "toast_credentials.json"
        self.credentials_path.write_text(
            json.dumps(
                {
                    "clientId": "client-id",
                    "clientSecret": "client-secret",
                    "apiHostname": "https://example.test",
                }
            )
        )
        self.client = ToastAPIClient(credentials_path=str(self.credentials_path))
        self.client._token = "token-123"
        self.client._token_expiry = object()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_default_headers_require_restaurant_by_default(self):
        self.client._ensure_token = Mock()

        with self.assertRaises(ValueError):
            self.client._default_headers()

    def test_default_headers_allow_restaurantless_requests_when_requested(self):
        self.client._ensure_token = Mock()

        headers = self.client._default_headers(require_restaurant=False)

        self.assertEqual("Bearer token-123", headers["Authorization"])
        self.assertNotIn("Toast-Restaurant-External-ID", headers)

    def test_request_retries_after_rate_limit(self):
        responses = [
            FakeResponse(status_code=429, headers={"Retry-After": "0"}),
            FakeResponse(json_data={"ok": True}),
        ]
        self.client._session.request = Mock(side_effect=responses)
        self.client._default_headers = Mock(return_value={"Authorization": "Bearer token-123"})

        with patch("integrations.toast_api.client.time.sleep") as sleep_mock:
            response = self.client._request("GET", "/orders")

        self.assertEqual({"ok": True}, response.json())
        self.assertEqual(2, self.client._session.request.call_count)
        sleep_mock.assert_called_once_with(0)

    def test_request_raises_after_exhausting_retries(self):
        self.client._session.request = Mock(
            return_value=FakeResponse(status_code=429, headers={"Retry-After": "0"})
        )
        self.client._default_headers = Mock(return_value={"Authorization": "Bearer token-123"})

        with patch("integrations.toast_api.client.time.sleep"):
            with self.assertRaises(RuntimeError):
                self.client._request("GET", "/orders", max_retries=2)

    def test_get_paginated_collects_all_pages(self):
        first = FakeResponse(
            json_data=[{"id": 1}, {"id": 2}],
            headers={"Toast-Next-Page-Token": "page-2"},
        )
        second = FakeResponse(json_data=[{"id": 3}], headers={})
        captured_params = []

        def fake_get(path, params=None):
            captured_params.append(dict(params or {}))
            return [first, second][len(captured_params) - 1]

        self.client.get = Mock(side_effect=fake_get)

        items = self.client.get_paginated("/menus/v2/menus", params={"pageSize": 50})

        self.assertEqual([{"id": 1}, {"id": 2}, {"id": 3}], items)
        self.assertEqual({"pageSize": 50}, captured_params[0])
        self.assertEqual({"pageSize": 50, "pageToken": "page-2"}, captured_params[1])

    def test_get_orders_bulk_formats_dates_and_stops_on_short_page(self):
        self.client.get = Mock(
            side_effect=[
                FakeResponse(json_data=[{"id": 1}, {"id": 2}]),
                FakeResponse(json_data=[{"id": 3}]),
            ]
        )

        with patch("integrations.toast_api.client.time.sleep") as sleep_mock:
            orders = self.client.get_orders_bulk("2026-04-01", "2026-04-02", page_size=2)

        self.assertEqual([{"id": 1}, {"id": 2}, {"id": 3}], orders)
        first_params = self.client.get.call_args_list[0].kwargs["params"]
        second_params = self.client.get.call_args_list[1].kwargs["params"]
        self.assertEqual("2026-04-01T00:00:00.000+0000", first_params["startDate"])
        self.assertEqual("2026-04-02T23:59:59.999+0000", first_params["endDate"])
        self.assertEqual(1, first_params["page"])
        self.assertEqual(2, second_params["page"])
        sleep_mock.assert_called_once_with(5)


if __name__ == "__main__":
    unittest.main()
