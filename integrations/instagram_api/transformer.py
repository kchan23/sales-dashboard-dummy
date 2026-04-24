"""Transform Instagram Graph API payloads into BigQuery rows."""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from integrations.instagram_api.client import format_timestamp_utc, parse_timestamp

JsonDict = Dict[str, Any]


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_snapshot_at(snapshot_at: Any) -> str:
    if isinstance(snapshot_at, datetime):
        return snapshot_at.astimezone(timezone.utc).isoformat(timespec="seconds")

    parsed = parse_timestamp(snapshot_at)
    if parsed is not None:
        return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")

    text = _clean_str(snapshot_at)
    if not text:
        raise ValueError("snapshot_at is required")
    return text


def _format_snapshot_date(snapshot_at: Any) -> str:
    if isinstance(snapshot_at, datetime):
        return snapshot_at.astimezone(timezone.utc).strftime("%Y%m%d")

    parsed = parse_timestamp(snapshot_at)
    if parsed is not None:
        return parsed.astimezone(timezone.utc).strftime("%Y%m%d")

    text = _clean_str(snapshot_at)
    if not text:
        raise ValueError("snapshot_at is required")
    return text[:10].replace("-", "")


def _normalize_child(child: JsonDict) -> JsonDict:
    parsed = parse_timestamp(child.get("timestamp"))
    return {
        "id": _clean_str(child.get("id")),
        "media_type": _clean_str(child.get("media_type")),
        "timestamp": _clean_str(child.get("timestamp")),
        "posted_at_utc": format_timestamp_utc(parsed),
        "posted_date_utc": parsed.astimezone(timezone.utc).strftime("%Y%m%d") if parsed else None,
        "media_url": _clean_str(child.get("media_url")),
        "thumbnail_url": _clean_str(child.get("thumbnail_url")),
    }


def _classify_media(item: JsonDict) -> Optional[str]:
    media_type = _clean_str(item.get("media_type"))
    product_type = (_clean_str(item.get("media_product_type")) or "").upper()
    normalized_media_type = (media_type or "").upper()

    if product_type == "REELS":
        return "REEL"
    if normalized_media_type == "VIDEO":
        return "VIDEO"
    if normalized_media_type == "IMAGE":
        return "IMAGE"
    if normalized_media_type == "CAROUSEL_ALBUM":
        return "CAROUSEL"
    return media_type


def transform_profile_snapshot(
    profile: JsonDict,
    account_label: Optional[str],
    run_id: str,
    snapshot_at: Any,
    *,
    local_timezone: Optional[str] = None,
) -> JsonDict:
    account_id = _clean_str(profile.get("id"))
    if not account_id:
        raise ValueError("Profile payload is missing required field 'id'")

    return {
        "account_id": account_id,
        "account_label": _clean_str(account_label) or _clean_str(profile.get("username")) or account_id,
        "username": _clean_str(profile.get("username")),
        "name": _clean_str(profile.get("name")),
        "biography": _clean_str(profile.get("biography")),
        "account_type": _clean_str(profile.get("account_type")),
        "media_count": _to_int(profile.get("media_count")),
        "followers_count": _to_int(profile.get("followers_count")),
        "follows_count": _to_int(profile.get("follows_count")),
        "profile_picture_url": _clean_str(profile.get("profile_picture_url")),
        "local_timezone": _clean_str(local_timezone),
        "snapshot_at": _format_snapshot_at(snapshot_at),
        "snapshot_date": _format_snapshot_date(snapshot_at),
        "source_run_id": run_id,
    }


def transform_media_snapshots(
    profile: JsonDict,
    media_list: List[JsonDict],
    account_label: Optional[str],
    run_id: str,
) -> List[JsonDict]:
    account_id = _clean_str(profile.get("id"))
    if not account_id:
        raise ValueError("Profile payload is missing required field 'id'")

    username = _clean_str(profile.get("username"))
    resolved_label = _clean_str(account_label) or username or account_id

    rows: List[JsonDict] = []
    for item in media_list:
        media_id = _clean_str(item.get("id"))
        if not media_id:
            continue

        posted_at = parse_timestamp(item.get("timestamp"))
        children_data = item.get("children", {}).get("data", [])
        if not isinstance(children_data, list):
            children_data = []

        normalized_children = [_normalize_child(child) for child in children_data if isinstance(child, dict)]

        rows.append(
            {
                "account_id": account_id,
                "account_label": resolved_label,
                "username": username,
                "media_id": media_id,
                "caption": _clean_str(item.get("caption")),
                "media_type": _classify_media(item),
                "media_product_type": _clean_str(item.get("media_product_type")),
                "permalink": _clean_str(item.get("permalink")),
                "media_url": _clean_str(item.get("media_url")),
                "thumbnail_url": _clean_str(item.get("thumbnail_url")),
                "posted_at_raw": _clean_str(item.get("timestamp")),
                "posted_at_utc": format_timestamp_utc(posted_at),
                "posted_date_utc": posted_at.astimezone(timezone.utc).strftime("%Y%m%d") if posted_at else None,
                "likes": _to_int(item.get("like_count", item.get("likes"))),
                "comments_count": _to_int(item.get("comments_count")),
                "views": _to_int(item.get("views")),
                "reach": _to_int(item.get("reach")),
                "saved": _to_int(item.get("saved")),
                "shares": _to_int(item.get("shares")),
                "total_interactions": _to_int(item.get("total_interactions")),
                "children_json": json.dumps(normalized_children, ensure_ascii=False) if normalized_children else None,
                "child_count": len(normalized_children),
                "source_run_id": run_id,
            }
        )

    return rows
