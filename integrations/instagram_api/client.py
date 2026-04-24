"""Instagram Graph API client helpers for BigQuery ingestion."""

import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

JsonDict = Dict[str, Any]

PROFILE_FIELDS = [
    "id",
    "username",
    "name",
    "biography",
    "account_type",
    "media_count",
    "followers_count",
    "follows_count",
    "profile_picture_url",
]

MIN_PROFILE_FIELDS = [
    "id",
    "username",
    "account_type",
    "media_count",
]

MEDIA_FIELDS = [
    "id",
    "caption",
    "media_type",
    "media_product_type",
    "media_url",
    "thumbnail_url",
    "permalink",
    "timestamp",
    "like_count",
    "comments_count",
    "children{media_type,media_url,thumbnail_url,id,timestamp}",
]

INSIGHT_METRICS = ["views", "reach", "saved", "shares", "total_interactions"]
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

logger = logging.getLogger(__name__)


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Config:
    access_token: str
    api_version: str
    graph_host: str
    timeout: int
    max_retries: int
    retry_backoff_seconds: int
    fetch_insights: bool
    media_limit: int
    local_timezone_name: str
    api_delay_seconds: float

    @property
    def base_url(self) -> str:
        return f"https://{self.graph_host}/{self.api_version}"

    @classmethod
    def from_env(cls) -> "Config":
        access_token = os.getenv("ACCESS_TOKEN", "").strip()
        if not access_token:
            raise RuntimeError("ACCESS_TOKEN not found in environment.")

        return cls(
            access_token=access_token,
            api_version=os.getenv("API_VERSION", "v25.0").strip(),
            graph_host=os.getenv("GRAPH_HOST", "graph.instagram.com").strip(),
            timeout=int(os.getenv("TIMEOUT", "30")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_backoff_seconds=int(os.getenv("RETRY_BACKOFF_SECONDS", "2")),
            fetch_insights=env_bool("FETCH_INSIGHTS", True),
            media_limit=int(os.getenv("MEDIA_LIMIT", "100")),
            local_timezone_name=os.getenv("LOCAL_TIMEZONE", "").strip(),
            api_delay_seconds=float(os.getenv("API_DELAY_SECONDS", "0")),
        )


class APIError(RuntimeError):
    """Graph API request failure."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        url: Optional[str] = None,
        payload: Any = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.url = url
        self.payload = payload


class InstagramGraphClient:
    """Thin wrapper around the Instagram Graph API."""

    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            }
        )

    def build_url(self, path_or_url: str) -> str:
        if path_or_url.startswith(("http://", "https://")):
            return path_or_url
        return f"{self.config.base_url}{path_or_url}"

    @staticmethod
    def safe_json(response: requests.Response) -> Optional[JsonDict]:
        try:
            return response.json()
        except Exception:
            return None

    def _sleep_seconds(self, response: Optional[requests.Response], attempt: int) -> int:
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                return int(retry_after)
        return self.config.retry_backoff_seconds * attempt

    @staticmethod
    def _raise_for_http_error(response: requests.Response, payload: Optional[JsonDict]) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise APIError(
                f"GET {response.url} failed with {response.status_code}: "
                f"{payload if payload is not None else response.text}",
                status_code=response.status_code,
                url=response.url,
                payload=payload,
            ) from exc

    def get_json(
        self,
        path_or_url: str,
        *,
        params: Optional[JsonDict] = None,
        include_access_token: bool = True,
        timeout: Optional[int] = None,
    ) -> JsonDict:
        url = self.build_url(path_or_url)
        query = dict(params or {})

        if include_access_token:
            query.setdefault("access_token", self.config.access_token)

        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                with self.session.get(url, params=query, timeout=timeout or self.config.timeout) as response:
                    payload = self.safe_json(response)

                    if response.status_code in RETRY_STATUS_CODES:
                        last_error = APIError(
                            f"GET {response.url} failed with {response.status_code}: "
                            f"{payload if payload is not None else response.text}",
                            status_code=response.status_code,
                            url=response.url,
                            payload=payload,
                        )
                        if attempt < self.config.max_retries:
                            time.sleep(self._sleep_seconds(response, attempt))
                            continue

                    self._raise_for_http_error(response, payload)

                    if payload is None:
                        raise APIError(
                            f"GET {response.url} returned non-JSON response",
                            status_code=response.status_code,
                            url=response.url,
                            payload=response.text[:1000],
                        )

                    if "error" in payload:
                        raise APIError(
                            f"Graph API error for {response.url}: {payload['error']}",
                            status_code=response.status_code,
                            url=response.url,
                            payload=payload,
                        )

                    return payload
            except requests.RequestException as exc:
                last_error = APIError(
                    f"GET {url} failed on attempt {attempt}/{self.config.max_retries}: {exc}",
                    url=url,
                )
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_backoff_seconds * attempt)
                    continue
            except APIError as exc:
                last_error = exc
                if attempt < self.config.max_retries and exc.status_code in RETRY_STATUS_CODES:
                    time.sleep(self.config.retry_backoff_seconds * attempt)
                    continue
                break

        raise last_error or APIError(
            f"GET {url} failed after {self.config.max_retries} attempts.",
            url=url,
        )

    def paginate(
        self,
        path_or_url: str,
        *,
        params: Optional[JsonDict] = None,
        include_access_token: bool = True,
    ) -> Iterable[JsonDict]:
        next_ref: Optional[str] = path_or_url
        next_params = params
        include_token = include_access_token

        while next_ref:
            data = self.get_json(
                next_ref,
                params=next_params,
                include_access_token=include_token,
            )

            rows = data.get("data", [])
            if not isinstance(rows, list):
                raise APIError(
                    f"Invalid paginated payload for {next_ref}: {data}",
                    url=self.build_url(next_ref),
                    payload=data,
                )

            for row in rows:
                yield row

            next_ref = data.get("paging", {}).get("next")
            next_params = None
            include_token = False


def normalize_timestamp_string(timestamp: Any) -> Optional[str]:
    if not timestamp:
        return None

    ts = str(timestamp).strip()

    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"

    match = re.match(r"^(.*)([+-]\d{2})(\d{2})$", ts)
    if match and ":" not in ts[-6:]:
        ts = f"{match.group(1)}{match.group(2)}:{match.group(3)}"

    return ts


def parse_timestamp(timestamp: Any) -> Optional[datetime]:
    normalized = normalize_timestamp_string(timestamp)
    if not normalized:
        return None

    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def parse_date_start(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    return datetime.strptime(date_str, "%Y-%m-%d").replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=timezone.utc,
    )


def parse_date_end(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    return datetime.combine(
        datetime.strptime(date_str, "%Y-%m-%d").date(),
        dt_time.max,
        tzinfo=timezone.utc,
    )


def format_timestamp_utc(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def maybe_sleep(config: Config) -> None:
    if config.api_delay_seconds > 0:
        time.sleep(config.api_delay_seconds)


def normalize_insight_value(insight: JsonDict) -> Any:
    if "value" in insight:
        return insight["value"]

    values = insight.get("values")
    if isinstance(values, list) and values:
        first = values[0]
        if isinstance(first, dict):
            return first.get("value")

    return None


def insight_metrics_for_item(item: JsonDict, config: Config) -> List[str]:
    if not config.fetch_insights:
        return []

    product_type = (item.get("media_product_type") or "").upper()
    if product_type not in {"FEED", "REELS", "STORY"}:
        return []

    return list(INSIGHT_METRICS)


def parse_insights_payload(data: JsonDict) -> JsonDict:
    result: JsonDict = {}

    for row in data.get("data", []):
        if not isinstance(row, dict):
            continue

        name = row.get("name")
        value = normalize_insight_value(row)
        if name and value is not None:
            result[name] = value

    return result


def fetch_single_metric(client: InstagramGraphClient, media_id: str, metric: str) -> Any:
    try:
        data = client.get_json(f"/{media_id}/insights", params={"metric": metric})
        return parse_insights_payload(data).get(metric)
    except Exception:
        return None


def fetch_insights(client: InstagramGraphClient, item: JsonDict) -> JsonDict:
    metrics = insight_metrics_for_item(item, client.config)
    if not metrics:
        return {}

    media_id = item.get("id")
    if not media_id:
        return {}

    try:
        data = client.get_json(
            f"/{media_id}/insights",
            params={"metric": ",".join(metrics)},
        )
        return parse_insights_payload(data)
    except Exception as exc:
        logger.warning("Bulk insights request failed for media %s: %s", media_id, exc)
        fallback_result: JsonDict = {}
        for metric in metrics:
            value = fetch_single_metric(client, media_id, metric)
            if value is not None:
                fallback_result[metric] = value
        return fallback_result


def get_profile(client: Optional[InstagramGraphClient] = None) -> JsonDict:
    graph_client = client or InstagramGraphClient(Config.from_env())
    try:
        return graph_client.get_json("/me", params={"fields": ",".join(PROFILE_FIELDS)})
    except Exception as exc:
        logger.warning("Expanded profile fetch failed, retrying with minimal fields: %s", exc)
        return graph_client.get_json("/me", params={"fields": ",".join(MIN_PROFILE_FIELDS)})


def get_media(
    client: Optional[InstagramGraphClient] = None,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    media_limit: Optional[int] = None,
) -> List[JsonDict]:
    graph_client = client or InstagramGraphClient(Config.from_env())
    config = graph_client.config
    start_dt = parse_date_start(start_date)
    end_dt = parse_date_end(end_date)

    results: List[JsonDict] = []
    for item in graph_client.paginate(
        "/me/media",
        params={
            "fields": ",".join(MEDIA_FIELDS),
            "limit": media_limit or config.media_limit,
        },
    ):
        item_dt = parse_timestamp(item.get("timestamp"))
        if item_dt is not None:
            item_dt = item_dt.astimezone(timezone.utc)
            if end_dt and item_dt > end_dt:
                continue
            if start_dt and item_dt < start_dt:
                break

        enriched = dict(item)
        enriched.update(fetch_insights(graph_client, enriched))
        results.append(enriched)
        maybe_sleep(config)

    return results
