#!/usr/bin/env python3
"""
Teammate-facing Toast pull helper.

Supports three destinations:
  - bigquery: existing production path
  - local: save transformed tables as parquet files
  - both: write to BigQuery and local parquet files

The local mode is intended for larger multi-store pulls where a teammate may
want to inspect or analyze data without immediately loading it into BigQuery.
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.bigquery import BigQueryManager
from toast_api.client import ToastAPIClient
from toast_api.scheduler import DEFAULT_INTERVAL_DAYS, compute_date_range, pull_restaurant
from toast_api.transformer import (
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


OUTPUT_ROOT = Path("local_toast_pulls")

# Sample-based planning constants from checked-in files:
# - `toast_api/test_output/orders_*.json`
# - `scripts/test_output/orders_*.json`
# - `toast_api/test_output/menus.json`
RAW_ORDERS_MB_PER_STORE_DAY = {
    "light": 1.8,
    "average": 2.6,
    "heavy": 4.0,
}
RAW_MENUS_MB_PER_STORE = 0.7

# Approximate parquet bytes-per-row from current `exports/` snapshot.
PARQUET_BYTES_PER_ROW = {
    "orders": 81,
    "order_items": 39,
    "payments": 27,
    "customer_orders": 95,
    "inventory": 4,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pull Toast API data to BigQuery, local parquet, or both."
    )
    parser.add_argument(
        "--restaurants",
        help="Comma-separated restaurant GUIDs. Defaults to all discovered restaurants.",
    )
    parser.add_argument(
        "--interval-days",
        type=int,
        default=DEFAULT_INTERVAL_DAYS,
        help="How many days back to pull if no prior imports exist (default: 30).",
    )
    parser.add_argument(
        "--start-date",
        help="Override start date (YYYYMMDD).",
    )
    parser.add_argument(
        "--end-date",
        help="Override end date (YYYYMMDD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--monthly",
        action="store_true",
        help="Pull the full previous calendar month.",
    )
    parser.add_argument(
        "--customer-only",
        action="store_true",
        help="Only save/write customer_orders rows.",
    )
    parser.add_argument(
        "--destination",
        choices=["bigquery", "local", "both"],
        default="bigquery",
        help="Where pulled data should be written.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_ROOT),
        help="Root directory for local parquet output.",
    )
    parser.add_argument(
        "--include-raw",
        action="store_true",
        help="In local mode, also save raw orders/menus API payloads as JSON.",
    )
    parser.add_argument(
        "--estimate-only",
        action="store_true",
        help="Print local storage estimates and exit without pulling data.",
    )
    return parser.parse_args()


def normalize_dates(args: argparse.Namespace) -> None:
    if args.monthly:
        today = datetime.now()
        last_day_prev = today.replace(day=1) - timedelta(days=1)
        first_day_prev = last_day_prev.replace(day=1)
        args.start_date = first_day_prev.strftime("%Y%m%d")
        args.end_date = last_day_prev.strftime("%Y%m%d")

    if args.start_date:
        datetime.strptime(args.start_date, "%Y%m%d")
    if args.end_date:
        datetime.strptime(args.end_date, "%Y%m%d")


def date_range_days(start_date: str, end_date: str) -> int:
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    return (end - start).days + 1


def slugify(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", text.strip().lower()).strip("_")
    return cleaned or "restaurant"


def estimate_local_storage(num_restaurants: int, num_days: int) -> Dict[str, float]:
    orders_light = RAW_ORDERS_MB_PER_STORE_DAY["light"] * num_restaurants * num_days
    orders_avg = RAW_ORDERS_MB_PER_STORE_DAY["average"] * num_restaurants * num_days
    orders_heavy = RAW_ORDERS_MB_PER_STORE_DAY["heavy"] * num_restaurants * num_days
    menus = RAW_MENUS_MB_PER_STORE * num_restaurants
    return {
        "light_mb": round(orders_light + menus, 1),
        "average_mb": round(orders_avg + menus, 1),
        "heavy_mb": round(orders_heavy + menus, 1),
    }


def print_estimate(num_restaurants: int, num_days: int) -> None:
    estimate = estimate_local_storage(num_restaurants, num_days)
    print("\nLocal storage estimate")
    print(f"Restaurants: {num_restaurants}")
    print(f"Days: {num_days}")
    print(
        "Raw Toast JSON planning range: "
        f"{estimate['light_mb']:.1f} MB light / "
        f"{estimate['average_mb']:.1f} MB average / "
        f"{estimate['heavy_mb']:.1f} MB heavy"
    )
    print(
        "Basis: checked-in sample order pulls are about 1.8 MB to 4.0 MB per store-day, "
        "plus about 0.7 MB per store for menus."
    )


def write_rows_as_parquet(rows: List[dict], path: Path) -> int:
    df = pd.DataFrame(rows)
    if df.empty:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path.stat().st_size


def save_local_pull(
    output_root: Path,
    restaurant_guid: str,
    restaurant_name: str,
    start_date: str,
    end_date: str,
    customer_only: bool,
    include_raw: bool,
    api_orders: List[dict],
    api_menus: List[dict],
) -> Dict[str, int]:
    safe_name = slugify(restaurant_name)
    run_dir = output_root / f"{safe_name}_{restaurant_guid[:8]}" / f"{start_date}_{end_date}"
    run_dir.mkdir(parents=True, exist_ok=True)

    sizes = {
        "orders": 0,
        "order_items": 0,
        "payments": 0,
        "customer_orders": 0,
        "inventory": 0,
        "raw_orders": 0,
        "raw_menus": 0,
    }

    order_rows = transform_orders(api_orders, restaurant_guid) if not customer_only else []
    item_rows = transform_order_items(api_orders, restaurant_guid) if not customer_only else []
    payment_rows = transform_payments(api_orders, restaurant_guid) if not customer_only else []
    customer_rows = transform_customer_orders(api_orders, restaurant_guid)
    menu_rows = transform_menus(api_menus, restaurant_guid, end_date) if (api_menus and not customer_only) else []

    sizes["orders"] = write_rows_as_parquet(order_rows, run_dir / "orders.parquet")
    sizes["order_items"] = write_rows_as_parquet(item_rows, run_dir / "order_items.parquet")
    sizes["payments"] = write_rows_as_parquet(payment_rows, run_dir / "payments.parquet")
    sizes["customer_orders"] = write_rows_as_parquet(customer_rows, run_dir / "customer_orders.parquet")
    sizes["inventory"] = write_rows_as_parquet(menu_rows, run_dir / "inventory.parquet")

    if include_raw:
        if api_orders:
            orders_path = run_dir / "orders_raw.json"
            orders_path.write_text(json.dumps(api_orders, indent=2))
            sizes["raw_orders"] = orders_path.stat().st_size
        if api_menus and not customer_only:
            menus_path = run_dir / "menus_raw.json"
            menus_path.write_text(json.dumps(api_menus, indent=2))
            sizes["raw_menus"] = menus_path.stat().st_size

    manifest = {
        "restaurant_guid": restaurant_guid,
        "restaurant_name": restaurant_name,
        "start_date": start_date,
        "end_date": end_date,
        "customer_only": customer_only,
        "files_written": {k: v for k, v in sizes.items() if v > 0},
        "row_counts": {
            "orders": len(order_rows),
            "order_items": len(item_rows),
            "payments": len(payment_rows),
            "customer_orders": len(customer_rows),
            "inventory": len(menu_rows),
        },
        "parquet_estimate_mb_from_row_counts": round(
            (
                len(order_rows) * PARQUET_BYTES_PER_ROW["orders"]
                + len(item_rows) * PARQUET_BYTES_PER_ROW["order_items"]
                + len(payment_rows) * PARQUET_BYTES_PER_ROW["payments"]
                + len(customer_rows) * PARQUET_BYTES_PER_ROW["customer_orders"]
                + len(menu_rows) * PARQUET_BYTES_PER_ROW["inventory"]
            )
            / (1024 * 1024),
            2,
        ),
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return {
        "run_dir": str(run_dir),
        "orders": len(order_rows),
        "order_items": len(item_rows),
        "payments": len(payment_rows),
        "customer_orders": len(customer_rows),
        "inventory": len(menu_rows),
        "bytes_written": sum(sizes.values()),
    }


def pull_restaurant_local(
    client: ToastAPIClient,
    bq: Optional[BigQueryManager],
    restaurant_guid: str,
    restaurant_name: str,
    interval_days: int,
    output_root: Path,
    start_date_override: Optional[str],
    end_date_override: Optional[str],
    customer_only: bool,
    include_raw: bool,
) -> Dict[str, object]:
    location_id = restaurant_guid
    date_range = None

    if bq is not None:
        date_range = compute_date_range(
            bq=bq,
            location_id=location_id,
            interval_days=interval_days,
            start_date_override=start_date_override,
            end_date_override=end_date_override,
        )

    if date_range is None:
        if start_date_override:
            end_date = end_date_override or (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            date_range = (start_date_override, end_date)
        else:
            end_date = end_date_override or (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=interval_days)).strftime("%Y%m%d")
            date_range = (start_date, end_date)

    start_date, end_date = date_range
    client.set_restaurant(restaurant_guid)

    api_start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
    api_end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
    api_orders = client.get_orders_bulk(api_start, api_end)
    api_menus = [] if customer_only else client.get_menus()

    local_stats = save_local_pull(
        output_root=output_root,
        restaurant_guid=restaurant_guid,
        restaurant_name=restaurant_name,
        start_date=start_date,
        end_date=end_date,
        customer_only=customer_only,
        include_raw=include_raw,
        api_orders=api_orders,
        api_menus=api_menus,
    )
    local_stats["status"] = "success"
    local_stats["date_range"] = f"{start_date} to {end_date}"
    local_stats["restaurant"] = restaurant_name
    local_stats["guid"] = restaurant_guid
    return local_stats


def main() -> None:
    args = parse_args()
    normalize_dates(args)

    client = ToastAPIClient()
    restaurants = client.discover_restaurants()
    name_map = {
        r.get("restaurantGuid", r.get("guid", "")): r.get("restaurantName", r.get("name", ""))
        for r in restaurants
    }

    if args.restaurants:
        selected_guids = [g.strip() for g in args.restaurants.split(",") if g.strip()]
        restaurants_to_process = [
            {"restaurantGuid": guid, "restaurantName": name_map.get(guid, guid)}
            for guid in selected_guids
        ]
    else:
        restaurants_to_process = restaurants

    if args.start_date:
        end_for_estimate = args.end_date or (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        num_days = date_range_days(args.start_date, end_for_estimate)
    else:
        num_days = args.interval_days

    if args.destination in ("local", "both") or args.estimate_only:
        print_estimate(len(restaurants_to_process), num_days)
        if args.estimate_only:
            return

    bq = None
    if args.destination in ("bigquery", "both", "local"):
        # Local mode can still use BigQuery import_log to compute the next missing range if available.
        try:
            bq = BigQueryManager()
            bq.create_schema()
        except Exception as exc:
            if args.destination == "bigquery":
                raise
            logger.warning(f"BigQuery unavailable for range lookup/logging; continuing local-only: {exc}")
            bq = None

    output_root = Path(args.output_dir)
    results: List[Dict[str, object]] = []

    for restaurant in restaurants_to_process:
        guid = restaurant.get("restaurantGuid", restaurant.get("guid", ""))
        name = restaurant.get("restaurantName", restaurant.get("name", guid))

        if args.destination == "bigquery":
            stats = pull_restaurant(
                client=client,
                bq=bq,
                restaurant_guid=guid,
                restaurant_name=name,
                interval_days=args.interval_days,
                dry_run=False,
                start_date_override=args.start_date,
                end_date_override=args.end_date,
                customer_only=args.customer_only,
            )
        elif args.destination == "local":
            stats = pull_restaurant_local(
                client=client,
                bq=bq,
                restaurant_guid=guid,
                restaurant_name=name,
                interval_days=args.interval_days,
                output_root=output_root,
                start_date_override=args.start_date,
                end_date_override=args.end_date,
                customer_only=args.customer_only,
                include_raw=args.include_raw,
            )
        else:
            bq_stats = pull_restaurant(
                client=client,
                bq=bq,
                restaurant_guid=guid,
                restaurant_name=name,
                interval_days=args.interval_days,
                dry_run=False,
                start_date_override=args.start_date,
                end_date_override=args.end_date,
                customer_only=args.customer_only,
            )
            local_stats = pull_restaurant_local(
                client=client,
                bq=bq,
                restaurant_guid=guid,
                restaurant_name=name,
                interval_days=args.interval_days,
                output_root=output_root,
                start_date_override=args.start_date,
                end_date_override=args.end_date,
                customer_only=args.customer_only,
                include_raw=args.include_raw,
            )
            stats = {
                "restaurant": name,
                "guid": guid,
                "status": "success" if bq_stats["status"] == "success" and local_stats["status"] == "success" else "partial",
                "date_range": local_stats["date_range"],
                "bigquery_orders": bq_stats["orders"],
                "local_orders": local_stats["orders"],
                "local_bytes_written": local_stats["bytes_written"],
                "run_dir": local_stats["run_dir"],
            }

        results.append(stats)

    print("\nPull summary")
    print(f"Destination: {args.destination}")
    for result in results:
        line = (
            f"- {result['restaurant']} | {result.get('status', 'unknown')} | "
            f"{result.get('date_range', 'n/a')}"
        )
        if "bytes_written" in result:
            line += f" | local={result['bytes_written'] / (1024 * 1024):.2f} MB"
        if "run_dir" in result:
            line += f" | {result['run_dir']}"
        print(line)


if __name__ == "__main__":
    main()
