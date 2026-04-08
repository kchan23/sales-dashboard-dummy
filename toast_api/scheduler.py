#!/usr/bin/env python3
"""
Toast API Scheduled Puller with Incremental Data Sync.

Pulls order, menu, and payment data from the Toast API for all (or specified)
restaurants, only fetching data that hasn't been imported yet.

Usage:
    # Default: pull last 30 days for all restaurants
    python -m toast_api.scheduler

    # Custom interval
    python -m toast_api.scheduler --interval-days 7

    # Specific restaurants only (comma-separated GUIDs)
    python -m toast_api.scheduler --restaurants GUID1,GUID2,GUID3

    # Dry run - show what would be pulled without writing to BigQuery
    python -m toast_api.scheduler --dry-run
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

from toast_api.client import ToastAPIClient
from toast_api.transformer import (
    transform_orders,
    transform_order_items,
    transform_payments,
    transform_customer_orders,
    transform_menus,
)
try:
    from database.bigquery import BigQueryManager
except ImportError:
    BigQueryManager = None  # Not available in standalone (alex/) distribution

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_DAYS = 30
DELAY_BETWEEN_RESTAURANTS = 2  # seconds between restaurant pulls


def compute_date_range(
    bq: BigQueryManager,
    location_id: str,
    interval_days: int,
    start_date_override: Optional[str] = None,
    end_date_override: Optional[str] = None,
) -> Optional[tuple]:
    """Determine the date range to pull for a restaurant.

    Checks import_log for the latest TOAST_API import date and calculates
    the gap to fill. If start_date_override is provided, skips the import_log
    check and uses it directly (useful for backfills).

    Returns:
        (start_date, end_date) as YYYYMMDD strings, or None if up to date.
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    end_date = end_date_override or yesterday

    if start_date_override:
        if start_date_override > end_date:
            return None
        return (start_date_override, end_date)

    latest = bq.get_latest_import_date(location_id, source="TOAST_API")

    if latest:
        # Start from the day after the last import
        latest_dt = datetime.strptime(str(latest), "%Y%m%d")
        start_dt = latest_dt + timedelta(days=1)
        start_date = start_dt.strftime("%Y%m%d")

        if start_date > end_date:
            return None  # Already up to date

        return (start_date, end_date)
    else:
        # No prior imports - pull from (today - interval_days) to yesterday
        start_dt = datetime.now() - timedelta(days=interval_days)
        start_date = start_dt.strftime("%Y%m%d")
        return (start_date, end_date)


def to_api_date(yyyymmdd: str) -> str:
    """Convert YYYYMMDD to YYYY-MM-DD for the Toast API."""
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"


def pull_restaurant(
    client: ToastAPIClient,
    bq: BigQueryManager,
    restaurant_guid: str,
    restaurant_name: str,
    interval_days: int,
    dry_run: bool = False,
    start_date_override: Optional[str] = None,
    end_date_override: Optional[str] = None,
    customer_only: bool = False,
) -> Dict:
    """Pull and import data for a single restaurant.

    Returns:
        Stats dict with orders, items, payments, menus row counts.
    """
    stats = {
        "restaurant": restaurant_name,
        "guid": restaurant_guid,
        "status": "skipped",
        "date_range": None,
        "orders": 0,
        "order_items": 0,
        "payments": 0,
        "customer_orders": 0,
        "menus": 0,
    }

    # Use restaurant GUID as location_id
    location_id = restaurant_guid

    # Determine what needs to be pulled
    date_range = compute_date_range(bq, location_id, interval_days, start_date_override, end_date_override)

    if date_range is None:
        logger.info(f"  {restaurant_name}: already up to date, skipping.")
        return stats

    start_date, end_date = date_range
    stats["date_range"] = f"{start_date} to {end_date}"
    stats["status"] = "dry_run" if dry_run else "pulling"

    logger.info(f"  {restaurant_name}: pulling {start_date} to {end_date}")

    if dry_run:
        return stats

    # Set restaurant context for API calls
    client.set_restaurant(restaurant_guid)

    # --- Pull Orders ---
    try:
        api_start = to_api_date(start_date)
        api_end = to_api_date(end_date)
        api_orders = client.get_orders_bulk(api_start, api_end)
        logger.info(f"    Fetched {len(api_orders)} orders from API")

        if api_orders:
            if not customer_only:
                # Transform and stream orders
                order_rows = transform_orders(api_orders, location_id)
                stats["orders"] = bq.stream_rows("orders", order_rows)

                # Transform and stream order items
                item_rows = transform_order_items(api_orders, location_id)
                stats["order_items"] = bq.stream_rows("order_items", item_rows)

                # Transform and stream payments
                payment_rows = transform_payments(api_orders, location_id)
                stats["payments"] = bq.stream_rows("payments", payment_rows)

            # Transform and stream customer order data (requires guest.pi:read scope)
            customer_rows = transform_customer_orders(api_orders, location_id)
            if customer_rows:
                stats["customer_orders"] = bq.stream_rows("customer_orders", customer_rows)

            # Log the import
            bq.log_import(
                location_id, end_date, "TOAST_API", "orders", stats["orders"]
            )

            logger.info(
                f"    Imported: {stats['orders']} orders, "
                f"{stats['order_items']} items, {stats['payments']} payments, "
                f"{stats['customer_orders']} customer_orders"
            )

    except Exception as e:
        logger.error(f"    Error pulling orders: {e}")
        stats["status"] = "error"

    # --- Pull Menus (not date-dependent, pull once) ---
    if customer_only:
        if stats["status"] != "error":
            stats["status"] = "success"
        _update_location_name_cache(restaurant_guid, restaurant_name)
        return stats

    try:
        api_menus = client.get_menus()
        if api_menus:
            snapshot_date = end_date
            menu_rows = transform_menus(api_menus, location_id, snapshot_date)
            stats["menus"] = bq.stream_rows("inventory", menu_rows)

            bq.log_import(
                location_id, snapshot_date, "TOAST_API", "menus", stats["menus"]
            )

            logger.info(f"    Imported: {stats['menus']} menu items")

    except Exception as e:
        logger.error(f"    Error pulling menus: {e}")

    if stats["status"] != "error":
        stats["status"] = "success"

    # Persist GUID → display name mapping so the dashboard can show names
    _update_location_name_cache(restaurant_guid, restaurant_name)

    return stats


def _update_location_name_cache(guid: str, name: str) -> None:
    """Write or update the GUID→name mapping in toast_api/location_names.json."""
    cache_path = Path(__file__).parent / "location_names.json"
    try:
        data = json.loads(cache_path.read_text()) if cache_path.exists() else {}
        data[guid] = name
        cache_path.write_text(json.dumps(data, indent=2))
    except Exception as e:
        logger.warning(f"Could not update location name cache: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Toast API scheduled puller with incremental sync"
    )
    parser.add_argument(
        "--interval-days",
        type=int,
        default=DEFAULT_INTERVAL_DAYS,
        help=f"How many days back to pull if no prior imports exist (default: {DEFAULT_INTERVAL_DAYS})",
    )
    parser.add_argument(
        "--restaurants",
        help="Comma-separated restaurant GUIDs. Defaults to all discovered restaurants.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be pulled without writing to BigQuery.",
    )
    parser.add_argument(
        "--start-date",
        help="Override start date for pull (YYYYMMDD). Bypasses import_log check — useful for backfills.",
    )
    parser.add_argument(
        "--end-date",
        help="Override end date for pull (YYYYMMDD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--monthly",
        action="store_true",
        help="Pull the entire previous calendar month (overrides --start-date and --end-date).",
    )
    parser.add_argument(
        "--customer-only",
        action="store_true",
        help="Only write customer_orders rows. Skips orders, order_items, payments, menus.",
    )
    args = parser.parse_args()

    # --monthly: compute first and last day of the previous calendar month
    if args.monthly:
        today = datetime.now()
        last_day_prev = today.replace(day=1) - timedelta(days=1)
        first_day_prev = last_day_prev.replace(day=1)
        args.start_date = first_day_prev.strftime("%Y%m%d")
        args.end_date = last_day_prev.strftime("%Y%m%d")

    print(f"Toast API Scheduled Puller")
    print(f"Interval: {args.interval_days} days")
    if args.monthly:
        print(f"Monthly mode: {args.start_date} to {args.end_date}")
    elif args.start_date:
        print(f"Start date override: {args.start_date}")
    if args.end_date and not args.monthly:
        print(f"End date override: {args.end_date}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print()

    # Initialize clients
    try:
        client = ToastAPIClient()
        bq = BigQueryManager()
        bq.create_schema()  # Ensure tables exist
    except Exception as e:
        print(f"ERROR initializing: {e}")
        sys.exit(1)

    # Determine which restaurants to process
    print("Discovering restaurants...")
    all_restaurants = client.discover_restaurants()
    name_map = {
        r.get("restaurantGuid", r.get("guid", "")): r.get("restaurantName", r.get("name", ""))
        for r in all_restaurants
    }

    if args.restaurants:
        # User-specified list — resolve names from API
        restaurant_guids = [g.strip() for g in args.restaurants.split(",")]
        restaurants = [
            {"restaurantGuid": g, "restaurantName": name_map.get(g, g)}
            for g in restaurant_guids
        ]
        logger.info(f"Processing {len(restaurants)} specified restaurants")
    else:
        restaurants = all_restaurants
        logger.info(f"Discovered {len(restaurants)} restaurants")

    print(f"Restaurants to process: {len(restaurants)}")
    print()

    # Process each restaurant
    all_stats: List[Dict] = []
    for i, restaurant in enumerate(restaurants):
        guid = restaurant.get("restaurantGuid", restaurant.get("guid", ""))
        name = restaurant.get("restaurantName", restaurant.get("name", guid))

        logger.info(f"[{i + 1}/{len(restaurants)}] Processing: {name}")

        stats = pull_restaurant(
            client=client,
            bq=bq,
            restaurant_guid=guid,
            restaurant_name=name,
            interval_days=args.interval_days,
            dry_run=args.dry_run,
            start_date_override=args.start_date,
            end_date_override=args.end_date,
            customer_only=args.customer_only,
        )
        all_stats.append(stats)

        # Delay between restaurants to respect rate limits
        if i < len(restaurants) - 1 and stats["status"] not in ("skipped", "dry_run"):
            time.sleep(DELAY_BETWEEN_RESTAURANTS)

    # --- Print Summary ---
    print(f"\n{'='*70}")
    print(f"PULL SUMMARY")
    print(f"{'='*70}")
    print(
        f"{'Restaurant':<30} {'Status':<10} {'Date Range':<25} {'Orders':>7} {'Items':>7} {'Payments':>8} {'Customers':>9} {'Menus':>6}"
    )
    print(f"{'─'*30} {'─'*10} {'─'*25} {'─'*7} {'─'*7} {'─'*8} {'─'*9} {'─'*6}")

    totals = {"orders": 0, "order_items": 0, "payments": 0, "customer_orders": 0, "menus": 0}

    for s in all_stats:
        name = s["restaurant"][:29]
        date_range = s["date_range"] or "up to date"
        print(
            f"  {name:<28} {s['status']:<10} {date_range:<25} "
            f"{s['orders']:>7} {s['order_items']:>7} {s['payments']:>8} {s['customer_orders']:>9} {s['menus']:>6}"
        )
        totals["orders"] += s["orders"]
        totals["order_items"] += s["order_items"]
        totals["payments"] += s["payments"]
        totals["customer_orders"] += s["customer_orders"]
        totals["menus"] += s["menus"]

    print(f"{'─'*30} {'─'*10} {'─'*25} {'─'*7} {'─'*7} {'─'*8} {'─'*9} {'─'*6}")
    print(
        f"  {'TOTAL':<28} {'':<10} {'':<25} "
        f"{totals['orders']:>7} {totals['order_items']:>7} {totals['payments']:>8} {totals['customer_orders']:>9} {totals['menus']:>6}"
    )

    skipped = sum(1 for s in all_stats if s["status"] == "skipped")
    errors = sum(1 for s in all_stats if s["status"] == "error")
    success = sum(1 for s in all_stats if s["status"] == "success")

    print(f"\nResults: {success} succeeded, {skipped} skipped (up to date), {errors} errors")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
