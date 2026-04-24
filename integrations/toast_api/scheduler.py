#!/usr/bin/env python3
"""
Toast API Scheduled Puller with Incremental Data Sync.

Pulls order, menu, and payment data from the Toast API for all (or specified)
restaurants, only fetching data that has not been imported yet.

Usage:
    python3 -m integrations.toast_api.scheduler
    python3 -m integrations.toast_api.scheduler --interval-days 7
    python3 -m integrations.toast_api.scheduler --restaurants GUID1,GUID2
    python3 -m integrations.toast_api.scheduler --dry-run
"""

import argparse
import json
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue
from typing import List, Dict, Optional

from database.bigquery import BigQueryManager
from integrations.toast_api.client import ToastAPIClient
from integrations.toast_api.transformer import (
    transform_customer_orders,
    transform_menus,
    transform_order_items,
    transform_orders,
    transform_payments,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_DAYS = 30
_location_name_lock = threading.Lock()


def compute_date_range(
    bq: BigQueryManager,
    location_id: str,
    interval_days: int,
    start_date_override: Optional[str] = None,
    end_date_override: Optional[str] = None,
) -> Optional[tuple]:
    """Determine the date range to pull for a restaurant."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    end_date = end_date_override or yesterday

    if start_date_override:
        if start_date_override > end_date:
            return None
        return (start_date_override, end_date)

    latest = bq.get_latest_import_date(location_id, source="TOAST_API")

    if latest:
        latest_dt = datetime.strptime(str(latest), "%Y%m%d")
        start_dt = latest_dt + timedelta(days=1)
        start_date = start_dt.strftime("%Y%m%d")
        if start_date > end_date:
            return None
        return (start_date, end_date)

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
    """Pull and import data for a single restaurant."""
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

    location_id = restaurant_guid
    date_range = compute_date_range(
        bq,
        location_id,
        interval_days,
        start_date_override,
        end_date_override,
    )

    if date_range is None:
        logger.info(f"  {restaurant_name}: already up to date, skipping.")
        return stats

    start_date, end_date = date_range
    stats["date_range"] = f"{start_date} to {end_date}"
    stats["status"] = "dry_run" if dry_run else "pulling"

    logger.info(f"  {restaurant_name}: pulling {start_date} to {end_date}")

    if dry_run:
        return stats

    client.set_restaurant(restaurant_guid)

    try:
        api_start = to_api_date(start_date)
        api_end = to_api_date(end_date)
        api_orders = client.get_orders_bulk(api_start, api_end)
        logger.info(f"    Fetched {len(api_orders)} orders from API")

        if api_orders:
            if not customer_only:
                order_rows = transform_orders(api_orders, location_id)
                stats["orders"] = bq.stream_rows("orders", order_rows)

                item_rows = transform_order_items(api_orders, location_id)
                stats["order_items"] = bq.stream_rows("order_items", item_rows)

                payment_rows = transform_payments(api_orders, location_id)
                stats["payments"] = bq.stream_rows("payments", payment_rows)

            customer_rows = transform_customer_orders(api_orders, location_id)
            if customer_rows:
                stats["customer_orders"] = bq.stream_rows("customer_orders", customer_rows)

            bq.log_import(location_id, end_date, "TOAST_API", "orders", stats["orders"])

            logger.info(
                f"    Imported: {stats['orders']} orders, "
                f"{stats['order_items']} items, {stats['payments']} payments, "
                f"{stats['customer_orders']} customer_orders"
            )

    except Exception as exc:
        logger.error(f"    Error pulling orders: {exc}")
        stats["status"] = "error"

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
            bq.log_import(location_id, snapshot_date, "TOAST_API", "menus", stats["menus"])
            logger.info(f"    Imported: {stats['menus']} menu items")

    except Exception as exc:
        logger.error(f"    Error pulling menus: {exc}")

    if stats["status"] != "error":
        stats["status"] = "success"

    _update_location_name_cache(restaurant_guid, restaurant_name)
    return stats


def _update_location_name_cache(guid: str, name: str) -> None:
    """Write or update the GUID→name mapping in integrations/toast_api/location_names.json."""
    cache_path = Path(__file__).parent / "location_names.json"
    with _location_name_lock:
        try:
            data = json.loads(cache_path.read_text()) if cache_path.exists() else {}
            data[guid] = name
            cache_path.write_text(json.dumps(data, indent=2))
        except Exception as exc:
            logger.warning(f"Could not update location name cache: {exc}")


def _pull_restaurant_worker(
    restaurant_guid: str,
    restaurant_name: str,
    bq: BigQueryManager,
    interval_days: int,
    dry_run: bool,
    start_date_override: Optional[str],
    end_date_override: Optional[str],
    customer_only: bool,
    auth_signal: Queue,
) -> Dict:
    """Authenticate one restaurant client, then run the pull."""
    try:
        client = ToastAPIClient()
        client.authenticate()
        auth_signal.put(restaurant_guid)
        client.set_restaurant(restaurant_guid)
        return pull_restaurant(
            client=client,
            bq=bq,
            restaurant_guid=restaurant_guid,
            restaurant_name=restaurant_name,
            interval_days=interval_days,
            dry_run=dry_run,
            start_date_override=start_date_override,
            end_date_override=end_date_override,
            customer_only=customer_only,
        )
    except Exception as exc:
        logger.error(f"Unhandled error for {restaurant_name} ({restaurant_guid}): {exc}")
        auth_signal.put(None)
        return {
            "restaurant": restaurant_name,
            "guid": restaurant_guid,
            "status": "error",
            "date_range": None,
            "orders": 0,
            "order_items": 0,
            "payments": 0,
            "customer_orders": 0,
            "menus": 0,
        }


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
        help="Override start date for pull (YYYYMMDD). Bypasses import_log check.",
    )
    parser.add_argument(
        "--end-date",
        help="Override end date for pull (YYYYMMDD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--monthly",
        action="store_true",
        help="Pull the entire previous calendar month.",
    )
    parser.add_argument(
        "--customer-only",
        action="store_true",
        help="Only write customer_orders rows.",
    )
    args = parser.parse_args()

    if args.monthly:
        today = datetime.now()
        last_day_prev = today.replace(day=1) - timedelta(days=1)
        first_day_prev = last_day_prev.replace(day=1)
        args.start_date = first_day_prev.strftime("%Y%m%d")
        args.end_date = last_day_prev.strftime("%Y%m%d")

    print("Toast API Scheduled Puller")
    print(f"Interval: {args.interval_days} days")
    if args.monthly:
        print(f"Monthly mode: {args.start_date} to {args.end_date}")
    elif args.start_date:
        print(f"Start date override: {args.start_date}")
    if args.end_date and not args.monthly:
        print(f"End date override: {args.end_date}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print()

    try:
        client = ToastAPIClient()
        bq = BigQueryManager()
        bq.create_schema()
    except Exception as exc:
        print(f"ERROR initializing: {exc}")
        sys.exit(1)

    print("Discovering restaurants...")
    all_restaurants = client.discover_restaurants()
    name_map = {
        r.get("restaurantGuid", r.get("guid", "")): r.get("restaurantName", r.get("name", ""))
        for r in all_restaurants
    }

    if args.restaurants:
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

    all_stats: List[Dict] = []
    auth_signal: Queue = Queue()
    remaining = list(restaurants)

    def _make_submit_kwargs(restaurant: Dict) -> Dict:
        return dict(
            restaurant_guid=restaurant.get("restaurantGuid", restaurant.get("guid", "")),
            restaurant_name=restaurant.get("restaurantName", restaurant.get("name", "")),
            bq=bq,
            interval_days=args.interval_days,
            dry_run=args.dry_run,
            start_date_override=args.start_date,
            end_date_override=args.end_date,
            customer_only=args.customer_only,
            auth_signal=auth_signal,
        )

    with ThreadPoolExecutor(max_workers=len(restaurants)) as executor:
        future_to_restaurant: Dict = {}

        def _submit(restaurant: Dict):
            future = executor.submit(_pull_restaurant_worker, **_make_submit_kwargs(restaurant))
            future_to_restaurant[future] = restaurant
            return future

        _submit(remaining.pop(0))

        for restaurant in remaining:
            auth_signal.get()
            _submit(restaurant)

        for future in as_completed(future_to_restaurant):
            restaurant = future_to_restaurant[future]
            name = restaurant.get("restaurantName", restaurant.get("name", ""))
            guid = restaurant.get("restaurantGuid", restaurant.get("guid", ""))
            try:
                stats = future.result()
            except Exception as exc:
                logger.error(f"Executor error for {name} ({guid}): {exc}")
                stats = {
                    "restaurant": name,
                    "guid": guid,
                    "status": "error",
                    "date_range": None,
                    "orders": 0,
                    "order_items": 0,
                    "payments": 0,
                    "customer_orders": 0,
                    "menus": 0,
                }
            all_stats.append(stats)
            logger.info(f"Completed: {stats['restaurant']} — {stats['status']}")

    print(f"\n{'='*70}")
    print("PULL SUMMARY")
    print(f"{'='*70}")
    print(
        f"{'Restaurant':<30} {'Status':<10} {'Date Range':<25} {'Orders':>7} {'Items':>7} {'Payments':>8} {'Customers':>9} {'Menus':>6}"
    )
    print(f"{'─'*30} {'─'*10} {'─'*25} {'─'*7} {'─'*7} {'─'*8} {'─'*9} {'─'*6}")

    totals = {"orders": 0, "order_items": 0, "payments": 0, "customer_orders": 0, "menus": 0}

    for stats in all_stats:
        name = stats["restaurant"][:29]
        date_range = stats["date_range"] or "up to date"
        print(
            f"  {name:<28} {stats['status']:<10} {date_range:<25} "
            f"{stats['orders']:>7} {stats['order_items']:>7} {stats['payments']:>8} {stats['customer_orders']:>9} {stats['menus']:>6}"
        )
        totals["orders"] += stats["orders"]
        totals["order_items"] += stats["order_items"]
        totals["payments"] += stats["payments"]
        totals["customer_orders"] += stats["customer_orders"]
        totals["menus"] += stats["menus"]

    print(f"{'─'*30} {'─'*10} {'─'*25} {'─'*7} {'─'*7} {'─'*8} {'─'*9} {'─'*6}")
    print(
        f"  {'TOTAL':<28} {'':<10} {'':<25} "
        f"{totals['orders']:>7} {totals['order_items']:>7} {totals['payments']:>8} {totals['customer_orders']:>9} {totals['menus']:>6}"
    )

    skipped = sum(1 for stats in all_stats if stats["status"] == "skipped")
    errors = sum(1 for stats in all_stats if stats["status"] == "error")
    success = sum(1 for stats in all_stats if stats["status"] == "success")

    print(f"\nResults: {success} succeeded, {skipped} skipped (up to date), {errors} errors")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
