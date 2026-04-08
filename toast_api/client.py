"""
Toast POS API client.

Handles OAuth authentication, token management, pagination, and rate limiting
for the Toast Platform API v2.
"""

import json
import time
import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

AUTH_ENDPOINT = "/authentication/v1/authentication/login"
RESTAURANTS_ENDPOINT = "/partners/v1/restaurants"
DEFAULT_PAGE_SIZE = 100


class ToastAPIClient:
    """Client for the Toast POS REST API."""

    def __init__(self, credentials_path: Optional[str] = None):
        """
        Initialize client by loading credentials from a JSON file.

        Args:
            credentials_path: Path to toast_credentials.json.
                              Falls back to TOAST_CREDENTIALS_PATH env var.
        """
        cred_path = credentials_path or os.getenv("TOAST_CREDENTIALS_PATH")
        if not cred_path:
            raise ValueError(
                "Toast credentials path not found. "
                "Set TOAST_CREDENTIALS_PATH in .env or pass credentials_path."
            )

        cred_file = Path(cred_path)
        if not cred_file.exists():
            raise FileNotFoundError(
                f"Credentials file not found: {cred_file}\n"
                "Copy toast_credentials.json.template to toast_credentials.json "
                "and fill in your values."
            )

        with open(cred_file) as f:
            creds = json.load(f)

        self.client_id = creds["clientId"]
        self.client_secret = creds["clientSecret"]
        self.user_access_type = creds.get("userAccessType", "TOAST_MACHINE_CLIENT")
        self.api_hostname = creds.get("apiHostname", "https://ws-api.toasttab.com")

        # Restaurant ID is set per-request or after discovery
        self.restaurant_external_id: Optional[str] = None

        # Token state
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

        # Session for connection pooling
        self._session = requests.Session()

        logger.info("Toast API client initialized.")

    def set_restaurant(self, restaurant_external_id: str):
        """Set the restaurant external ID for subsequent API calls."""
        self.restaurant_external_id = restaurant_external_id
        logger.info(f"Restaurant set to: {restaurant_external_id}")

    def _authenticate(self):
        """Obtain a new OAuth token via client credentials grant."""
        url = f"{self.api_hostname}{AUTH_ENDPOINT}"
        payload = {
            "clientId": self.client_id,
            "clientSecret": self.client_secret,
            "userAccessType": self.user_access_type,
        }

        logger.info("Authenticating with Toast API...")
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        self._token = data["token"]["accessToken"]

        # Parse expiry - Toast returns expiresIn in seconds (typically 86400)
        expires_in = data["token"].get("expiresIn", 86400)
        # Refresh 5 minutes early to avoid edge cases
        self._token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)

        logger.info(f"Authenticated. Token expires in {expires_in}s.")

    def _ensure_token(self):
        """Refresh the token if it's missing or about to expire."""
        if self._token is None or datetime.now() >= self._token_expiry:
            self._authenticate()

    def _default_headers(self, require_restaurant: bool = True) -> Dict[str, str]:
        """Build headers required for Toast API requests."""
        self._ensure_token()
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        if self.restaurant_external_id:
            headers["Toast-Restaurant-External-ID"] = self.restaurant_external_id
        elif require_restaurant:
            raise ValueError(
                "Restaurant external ID not set. "
                "Call set_restaurant() or run --discover first."
            )
        return headers

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        require_restaurant: bool = True,
        max_retries: int = 3,
    ) -> requests.Response:
        """
        Make an API request with rate-limit retry.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API path (e.g. /orders/v2/ordersBulk)
            params: Query parameters
            require_restaurant: Whether to require restaurant ID header
            max_retries: Number of retries on 429

        Returns:
            requests.Response
        """
        url = f"{self.api_hostname}{path}"
        headers = self._default_headers(require_restaurant=require_restaurant)

        for attempt in range(max_retries + 1):
            resp = self._session.request(
                method, url, headers=headers, params=params, timeout=60
            )

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 5))
                logger.warning(
                    f"Rate limited (429). Waiting {wait}s... (attempt {attempt + 1})"
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        raise RuntimeError(f"Max retries exceeded for {method} {path}")

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        require_restaurant: bool = True,
    ) -> requests.Response:
        """Convenience GET request."""
        return self._request(
            "GET", path, params=params, require_restaurant=require_restaurant
        )

    def get_paginated(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """
        Fetch all pages from a paginated endpoint using Toast-Next-Page-Token.

        Returns a flat list of all items across all pages.
        """
        params = dict(params or {})
        all_items = []
        page = 0

        while True:
            resp = self.get(path, params=params)
            data = resp.json()

            if isinstance(data, list):
                all_items.extend(data)
            else:
                all_items.append(data)

            # Check for next page token in response headers
            next_token = resp.headers.get("Toast-Next-Page-Token")
            if not next_token:
                break

            params["pageToken"] = next_token
            page += 1
            logger.info(f"  Fetching page {page + 1}...")

        return all_items

    # --- Restaurant Discovery ---

    def discover_restaurants(self) -> List[Dict]:
        """
        Discover all restaurants accessible with these credentials.
        Uses GET /partners/v1/restaurants (no restaurant ID header needed).
        Follows Toast-Next-Page-Token pagination to return the full list.

        Returns:
            List of restaurant dicts with guid, name, etc.
        """
        params: Dict[str, Any] = {}
        all_restaurants: List[Dict] = []
        page = 0

        while True:
            resp = self.get(RESTAURANTS_ENDPOINT, params=params, require_restaurant=False)
            data = resp.json()
            if isinstance(data, list):
                all_restaurants.extend(data)
            else:
                all_restaurants.append(data)

            next_token = resp.headers.get("Toast-Next-Page-Token")
            if not next_token:
                break

            params["pageToken"] = next_token
            page += 1
            logger.info(f"  Fetching restaurants page {page + 1}...")

        return all_restaurants

    # --- Data Endpoints (require restaurant ID) ---

    def get_orders_bulk(
        self,
        start_date: str,
        end_date: str,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> List[Dict]:
        """
        Fetch orders for a date range using /orders/v2/ordersBulk.

        Args:
            start_date: ISO date string (YYYY-MM-DD) or ISO datetime
            end_date: ISO date string (YYYY-MM-DD) or ISO datetime
            page_size: Number of orders per page (max varies by endpoint)

        Returns:
            List of order dicts
        """
        # Toast expects ISO 8601 datetime format
        if len(start_date) == 10:
            start_date = f"{start_date}T00:00:00.000+0000"
        if len(end_date) == 10:
            end_date = f"{end_date}T23:59:59.999+0000"

        all_orders = []
        page = 1

        while True:
            params = {
                "startDate": start_date,
                "endDate": end_date,
                "pageSize": page_size,
                "page": page,
            }
            resp = self.get("/orders/v2/ordersBulk", params=params)
            orders = resp.json()

            if not orders:
                break

            all_orders.extend(orders)
            logger.info(f"  Orders page {page}: fetched {len(orders)} orders")

            # Check if we got fewer than page_size (last page)
            if len(orders) < page_size:
                break

            page += 1
            # Toast recommends 5-10s between bulk requests
            time.sleep(5)

        return all_orders

    def get_menus(self) -> List[Dict]:
        """Fetch full menu data from /menus/v2/menus."""
        return self.get_paginated("/menus/v2/menus")

    def get_prep_stations(self) -> List[Dict]:
        """Fetch prep station data from /config/v2/prepStations."""
        return self.get_paginated("/kitchen/v1/published/prepStations")
