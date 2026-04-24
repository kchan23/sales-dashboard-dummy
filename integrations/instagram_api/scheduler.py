#!/usr/bin/env python3
"""Instagram Graph API scheduled puller for BigQuery snapshots."""

import argparse
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from database.bigquery import BigQueryManager
from integrations.instagram_api.client import (
    Config,
    InstagramGraphClient,
    get_media,
    get_profile,
    parse_date_end,
    parse_date_start,
)
from integrations.instagram_api.transformer import (
    transform_media_snapshots,
    transform_profile_snapshot,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Instagram Graph API snapshot sync")
    parser.add_argument(
        "--account-label",
        default=os.getenv("INSTAGRAM_ACCOUNT_LABEL", "").strip() or None,
        help="Human-friendly label for this Instagram account.",
    )
    parser.add_argument(
        "--start-date",
        help="Explicit inclusive start date for media pull (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        help="Explicit inclusive end date for media pull (YYYY-MM-DD). Defaults to today in UTC.",
    )
    parser.add_argument(
        "--refresh-days",
        type=int,
        default=int(os.getenv("IG_REFRESH_DAYS", "30")),
        help="Rolling refresh window in days when no explicit dates are provided.",
    )
    parser.add_argument(
        "--media-limit",
        type=int,
        help="Page size for /me/media requests. Defaults to MEDIA_LIMIT from env.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and transform only. Do not write to BigQuery.",
    )
    return parser.parse_args(argv)


def _validate_args(args) -> None:
    if args.refresh_days <= 0:
        raise ValueError("--refresh-days must be greater than zero")

    if args.start_date:
        try:
            parse_date_start(args.start_date)
        except ValueError as exc:
            raise ValueError("--start-date must be in YYYY-MM-DD format") from exc

    if args.end_date:
        try:
            parse_date_end(args.end_date)
        except ValueError as exc:
            raise ValueError("--end-date must be in YYYY-MM-DD format") from exc


def compute_window(
    *,
    start_date: Optional[str],
    end_date: Optional[str],
    refresh_days: int,
    now: datetime,
) -> Dict[str, str]:
    if start_date or end_date:
        effective_end = end_date or now.astimezone(timezone.utc).strftime("%Y-%m-%d")
        effective_start = start_date or effective_end
    else:
        effective_end = now.astimezone(timezone.utc).strftime("%Y-%m-%d")
        effective_start = (now.astimezone(timezone.utc) - timedelta(days=refresh_days)).strftime("%Y-%m-%d")

    if effective_start > effective_end:
        raise ValueError("start date must be on or before end date")

    return {"start_date": effective_start, "end_date": effective_end}


def run_sync(
    *,
    account_label: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    refresh_days: int = 30,
    media_limit: Optional[int] = None,
    dry_run: bool = False,
    bq: Optional[BigQueryManager] = None,
    client: Optional[InstagramGraphClient] = None,
    now: Optional[datetime] = None,
) -> Dict:
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    window = compute_window(
        start_date=start_date,
        end_date=end_date,
        refresh_days=refresh_days,
        now=now_utc,
    )

    graph_client = client or InstagramGraphClient(Config.from_env())
    run_id = str(uuid.uuid4())
    run_date = now_utc.strftime("%Y%m%d")

    profile = get_profile(graph_client)
    resolved_label = (account_label or "").strip() or os.getenv("INSTAGRAM_ACCOUNT_LABEL", "").strip() or profile.get("username") or profile.get("id")

    profile_row = transform_profile_snapshot(
        profile,
        resolved_label,
        run_id,
        now_utc,
        local_timezone=graph_client.config.local_timezone_name,
    )
    media = get_media(
        graph_client,
        start_date=window["start_date"],
        end_date=window["end_date"],
        media_limit=media_limit,
    )
    media_rows = transform_media_snapshots(profile, media, resolved_label, run_id)

    stats = {
        "account_id": profile_row["account_id"],
        "account_label": resolved_label,
        "status": "dry_run" if dry_run else "success",
        "window_start": window["start_date"],
        "window_end": window["end_date"],
        "profile_rows": 1,
        "media_rows": len(media_rows),
        "run_id": run_id,
    }

    logger.info(
        "Instagram sync prepared for %s (%s): %s profile row, %s media rows, window %s to %s",
        resolved_label,
        profile_row["account_id"],
        stats["profile_rows"],
        stats["media_rows"],
        window["start_date"],
        window["end_date"],
    )

    if dry_run:
        return stats

    warehouse = bq or BigQueryManager()
    warehouse.create_schema()

    inserted_profiles = warehouse.stream_rows("instagram_profile_snapshots", [profile_row])
    inserted_media = warehouse.stream_rows("instagram_media_snapshots", media_rows)

    warehouse.log_import(
        profile_row["account_id"],
        run_date,
        "INSTAGRAM_API_PROFILE",
        "instagram_profile_snapshots",
        inserted_profiles,
    )
    warehouse.log_import(
        profile_row["account_id"],
        run_date,
        "INSTAGRAM_API_MEDIA",
        "instagram_media_snapshots",
        inserted_media,
    )

    stats["profile_rows"] = inserted_profiles
    stats["media_rows"] = inserted_media
    return stats


def main(argv=None) -> None:
    try:
        args = parse_args(argv)
        _validate_args(args)
        stats = run_sync(
            account_label=args.account_label,
            start_date=args.start_date,
            end_date=args.end_date,
            refresh_days=args.refresh_days,
            media_limit=args.media_limit,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    print("Instagram Graph API Snapshot Sync")
    print(f"Account: {stats['account_label']} ({stats['account_id']})")
    print(f"Window: {stats['window_start']} to {stats['window_end']}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"Profile rows: {stats['profile_rows']}")
    print(f"Media rows: {stats['media_rows']}")
    print(f"Run ID: {stats['run_id']}")


if __name__ == "__main__":
    main()
