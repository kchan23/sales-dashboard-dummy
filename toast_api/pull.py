#!/usr/bin/env python3
"""
Toast API pull script — BigQuery or local file output.

Extends the existing scheduler with a --local flag that writes data to
CSV or Parquet files instead of streaming to BigQuery. Includes a
--estimate-only mode that prints projected file sizes before any pull.

Usage:
    # Pull to BigQuery (default, same as scheduler.py)
    python -m toast_api.pull --interval-days 30

    # Pull locally as Parquet (default local format)
    python -m toast_api.pull --local --output-dir ./data/toast_export

    # Pull locally as CSV
    python -m toast_api.pull --local --format csv --output-dir ./data/toast_export

    # Estimate file sizes before committing to a pull
    python -m toast_api.pull --local --estimate-only --num-stores 5 --interval-days 90

    # Backfill a specific date range locally
    python -m toast_api.pull --local --start-date 20240101 --end-date 20240331

    # Specific restaurants only
    python -m toast_api.pull --local --restaurants GUID1,GUID2 --interval-days 7
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import pandas as pd

from toast_api.client import ToastAPIClient
from toast_api.transformer import (
    transform_orders,
    transform_order_items,
    transform_payments,
    transform_customer_orders,
    transform_menus,
)

# NOTE: scheduler.py is NOT imported at the top level because it pulls in
# database/bigquery.py which requires streamlit. These two pure utilities are
# copied inline so the alex venv (no streamlit) can run --local and
# --estimate-only without any BigQuery/Streamlit dependency.

def to_api_date(yyyymmdd: str) -> str:
    """Convert YYYYMMDD to YYYY-MM-DD for the Toast API."""
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"


def _update_location_name_cache(guid: str, name: str) -> None:
    """Write or update the GUID→name mapping in toast_api/location_names.json."""
    cache_path = Path(__file__).parent / "location_names.json"
    try:
        data = json.loads(cache_path.read_text()) if cache_path.exists() else {}
        data[guid] = name
        cache_path.write_text(json.dumps(data, indent=2))
    except Exception as e:
        logger.warning(f"Could not update location name cache: {e}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_DAYS = 30
DELAY_BETWEEN_RESTAURANTS = 2  # seconds

# ---------------------------------------------------------------------------
# File size estimation constants
# These are conservative estimates based on typical quick-service restaurant
# data density. Actual sizes depend on menu complexity, check item counts,
# and caption/modifier verbosity.
# ---------------------------------------------------------------------------

# Approximate uncompressed CSV bytes per row
BYTES_PER_ROW = {
    "orders":          700,   # timestamps, GUIDs, totals, order type, location
    "order_items":     420,   # item name, modifiers, price, quantity, GUID refs
    "payments":        480,   # payment method, amount, tip, card type, GUID
    "customer_orders": 310,   # customer GUID, email hash, loyalty points
    "menus":           580,   # item name, group, category, price, description
}

# Parquet compresses roughly 4-6x vs uncompressed CSV for this data profile.
PARQUET_COMPRESSION_RATIO = 5.0
CSV_COMPRESSION_RATIO = 1.0  # uncompressed

# Approximate order:item:payment multipliers per order
ITEMS_PER_ORDER = 3.8
PAYMENTS_PER_ORDER = 1.1
CUSTOMER_ROWS_PER_ORDER = 0.7  # not every order has identifiable customer data

# One-time menu snapshot per location pull (rows, not per-day)
MENU_ROWS_PER_LOCATION = 800


def estimate_sizes(
    num_stores: int,
    interval_days: int,
    orders_per_day: int,
    fmt: str = "parquet",
) -> Dict:
    """
    Return a dict of estimated file sizes (bytes) broken down by table.

    Args:
        num_stores:      Number of restaurant locations.
        interval_days:   Number of days being pulled.
        orders_per_day:  Estimated daily order count per store.
        fmt:             'parquet' or 'csv' — determines compression factor.

    Returns:
        Dict with keys per table and a 'total' key, all in bytes.
    """
    ratio = PARQUET_COMPRESSION_RATIO if fmt == "parquet" else CSV_COMPRESSION_RATIO
    total_orders = num_stores * interval_days * orders_per_day

    raw = {
        "orders":          total_orders * BYTES_PER_ROW["orders"],
        "order_items":     total_orders * ITEMS_PER_ORDER * BYTES_PER_ROW["order_items"],
        "payments":        total_orders * PAYMENTS_PER_ORDER * BYTES_PER_ROW["payments"],
        "customer_orders": total_orders * CUSTOMER_ROWS_PER_ORDER * BYTES_PER_ROW["customer_orders"],
        "menus":           num_stores * MENU_ROWS_PER_LOCATION * BYTES_PER_ROW["menus"],
    }

    compressed = {k: int(v / ratio) for k, v in raw.items()}
    compressed["total"] = sum(compressed.values())
    compressed["_raw_total"] = int(sum(raw.values()))
    return compressed


def _human(n_bytes: int) -> str:
    """Format bytes as a human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n_bytes < 1024:
            return f"{n_bytes:.1f} {unit}"
        n_bytes /= 1024
    return f"{n_bytes:.1f} PB"


def print_size_estimate(
    num_stores: int,
    interval_days: int,
    orders_per_day: int,
    fmt: str,
) -> None:
    """Print a formatted file-size estimate table to stdout."""
    sizes = estimate_sizes(num_stores, interval_days, orders_per_day, fmt)
    ext = "parquet" if fmt == "parquet" else "csv"
    compression_note = f"~{PARQUET_COMPRESSION_RATIO:.0f}x compressed" if fmt == "parquet" else "uncompressed"

    print()
    print("=" * 62)
    print("  FILE SIZE ESTIMATE")
    print("=" * 62)
    print(f"  Stores:          {num_stores}")
    print(f"  Days:            {interval_days}")
    print(f"  Orders/day/store:{orders_per_day:>5}  (adjust with --orders-per-day)")
    print(f"  Format:          .{ext}  ({compression_note})")
    print(f"  Total orders est:{num_stores * interval_days * orders_per_day:>7,}")
    print()
    print(f"  {'Table':<20} {'Est. size':>12}  {'File pattern'}")
    print(f"  {'-'*20} {'-'*12}  {'-'*22}")
    tables = ["orders", "order_items", "payments", "customer_orders", "menus"]
    for t in tables:
        pattern = f"{t}_<location>.{ext}"
        print(f"  {t:<20} {_human(sizes[t]):>12}  {pattern}")
    print(f"  {'-'*20} {'-'*12}")
    print(f"  {'TOTAL (compressed)':<20} {_human(sizes['total']):>12}")
    print(f"  {'TOTAL (raw CSV ref)':<20} {_human(sizes['_raw_total']):>12}")
    print("=" * 62)
    print()
    print("  NOTE: Estimates assume average quick-service volume.")
    print("  Actual sizes vary ±50% based on modifier verbosity,")
    print("  item count per order, and customer data coverage.")
    print()


# ---------------------------------------------------------------------------
# Local file writer
# ---------------------------------------------------------------------------

def write_rows_local(
    rows: List[Dict],
    table_name: str,
    location_id: str,
    output_dir: Path,
    fmt: str,
    append: bool = True,
) -> int:
    """
    Write transformed row dicts to a local file.

    One file per (table, location) pair. On subsequent pulls for the same
    location, rows are appended (Parquet via concat + rewrite; CSV via mode='a').

    Args:
        rows:       List of row dicts (same format as BigQuery stream rows).
        table_name: e.g. 'orders', 'order_items'
        location_id: Restaurant GUID used to namespace the file.
        output_dir: Directory to write into.
        fmt:        'parquet' or 'csv'
        append:     If True, append to existing file. If False, overwrite.

    Returns:
        Number of rows written.
    """
    if not rows:
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    safe_loc = location_id.replace("-", "")[:16]
    ext = "parquet" if fmt == "parquet" else "csv"
    out_path = output_dir / f"{table_name}_{safe_loc}.{ext}"

    new_df = pd.DataFrame(rows)

    if fmt == "parquet":
        if append and out_path.exists():
            existing = pd.read_parquet(out_path)
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined.to_parquet(out_path, index=False, compression="snappy")
        else:
            new_df.to_parquet(out_path, index=False, compression="snappy")
    else:
        write_mode = "a" if append and out_path.exists() else "w"
        header = not (append and out_path.exists())
        new_df.to_csv(out_path, mode=write_mode, header=header, index=False)

    logger.info(f"    Wrote {len(rows)} rows → {out_path}")
    return len(rows)


# ---------------------------------------------------------------------------
# Per-restaurant pull (local mode)
# ---------------------------------------------------------------------------

def pull_restaurant_local(
    client: ToastAPIClient,
    restaurant_guid: str,
    restaurant_name: str,
    start_date: str,
    end_date: str,
    output_dir: Path,
    fmt: str,
    dry_run: bool = False,
    customer_only: bool = False,
) -> Dict:
    """Pull and save data for one restaurant to local files."""
    stats = {
        "restaurant": restaurant_name,
        "guid": restaurant_guid,
        "status": "dry_run" if dry_run else "pulling",
        "date_range": f"{start_date} to {end_date}",
        "orders": 0,
        "order_items": 0,
        "payments": 0,
        "customer_orders": 0,
        "menus": 0,
    }

    if dry_run:
        return stats

    location_id = restaurant_guid
    client.set_restaurant(restaurant_guid)

    # Pull orders
    try:
        api_orders = client.get_orders_bulk(to_api_date(start_date), to_api_date(end_date))
        logger.info(f"    Fetched {len(api_orders)} raw orders from API")

        if api_orders:
            if not customer_only:
                stats["orders"] = write_rows_local(
                    transform_orders(api_orders, location_id),
                    "orders", location_id, output_dir, fmt,
                )
                stats["order_items"] = write_rows_local(
                    transform_order_items(api_orders, location_id),
                    "order_items", location_id, output_dir, fmt,
                )
                stats["payments"] = write_rows_local(
                    transform_payments(api_orders, location_id),
                    "payments", location_id, output_dir, fmt,
                )

            customer_rows = transform_customer_orders(api_orders, location_id)
            if customer_rows:
                stats["customer_orders"] = write_rows_local(
                    customer_rows, "customer_orders", location_id, output_dir, fmt,
                )

    except Exception as e:
        logger.error(f"    Error pulling orders for {restaurant_name}: {e}")
        stats["status"] = "error"
        return stats

    # Pull menus (not customer-only)
    if not customer_only:
        try:
            api_menus = client.get_menus()
            if api_menus:
                menu_rows = transform_menus(api_menus, location_id, end_date)
                stats["menus"] = write_rows_local(
                    menu_rows, "menus", location_id, output_dir, fmt,
                )
        except Exception as e:
            logger.error(f"    Error pulling menus for {restaurant_name}: {e}")

    stats["status"] = "success"
    _update_location_name_cache(restaurant_guid, restaurant_name)
    return stats


# ---------------------------------------------------------------------------
# Chunk generator for backfill mode
# ---------------------------------------------------------------------------

def _backfill_chunks(n_years: int, chunk_size: str) -> List[tuple]:
    """
    Return a list of (start_date, end_date) YYYYMMDD string pairs covering
    the last n_years of complete calendar periods, oldest-first.

    chunk_size: 'year' | 'quarter' | 'month'
    """
    today = datetime.now().date()
    # End at end of last complete period before today
    chunks = []

    if chunk_size == "year":
        for y in range(n_years, 0, -1):
            year = today.year - y
            chunks.append((f"{year}0101", f"{year}1231"))

    elif chunk_size == "quarter":
        for y in range(n_years, 0, -1):
            year = today.year - y
            for q_start, q_end in [("0101", "0331"), ("0401", "0630"),
                                    ("0701", "0930"), ("1001", "1231")]:
                chunks.append((f"{year}{q_start}", f"{year}{q_end}"))

    elif chunk_size == "month":
        for y in range(n_years, 0, -1):
            year = today.year - y
            for month in range(1, 13):
                import calendar
                last_day = calendar.monthrange(year, month)[1]
                chunks.append((
                    f"{year}{month:02d}01",
                    f"{year}{month:02d}{last_day:02d}",
                ))

    # Drop any chunks entirely in the future
    today_str = today.strftime("%Y%m%d")
    return [(s, min(e, today_str)) for s, e in chunks if s <= today_str]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Toast API pull — BigQuery or local file output",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Date range
    parser.add_argument("--interval-days", type=int, default=DEFAULT_INTERVAL_DAYS,
                        help=f"Days back to pull if no prior data exists (default: {DEFAULT_INTERVAL_DAYS})")
    parser.add_argument("--start-date", help="Override start date (YYYYMMDD). Bypasses import_log check.")
    parser.add_argument("--end-date", help="Override end date (YYYYMMDD). Defaults to yesterday.")
    parser.add_argument("--monthly", action="store_true",
                        help="Pull the entire previous calendar month.")

    # Restaurant selection
    parser.add_argument("--restaurants",
                        help="Comma-separated restaurant GUIDs. Defaults to all.")
    parser.add_argument("--customer-only", action="store_true",
                        help="Only write customer_orders rows.")

    # Output destination
    parser.add_argument("--local", action="store_true",
                        help="Write to local files instead of BigQuery.")
    parser.add_argument("--output-dir", default="./data/toast_export",
                        help="Output directory for local files (default: ./data/toast_export).")
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet",
                        help="Local file format: parquet (default, smaller) or csv.")

    # Estimation
    parser.add_argument("--estimate-only", action="store_true",
                        help="Print file size estimates and exit. No data is pulled.")
    parser.add_argument("--num-stores", type=int, default=None,
                        help="Number of stores for --estimate-only (defaults to discovered count).")
    parser.add_argument("--orders-per-day", type=int, default=300,
                        help="Est. daily orders per store for size estimation (default: 300).")

    # Chunked backfill
    parser.add_argument("--backfill-years", type=int, default=None,
                        help="Pull N full calendar years back from yesterday, one year at a time. "
                             "Each year is a separate pull with its own summary. "
                             "Incompatible with --start-date / --end-date / --monthly.")
    parser.add_argument("--chunk-size", choices=["year", "month", "quarter"], default="year",
                        help="Chunk size for --backfill-years (default: year).")

    # Misc
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be pulled without writing anything.")
    parser.add_argument("--discover", action="store_true", default=False,
                        help="List all restaurants accessible with these credentials and exit.")

    args = parser.parse_args()

    # Validate mutual exclusion: --backfill-years vs explicit date flags
    if args.backfill_years and (args.start_date or args.end_date or args.monthly):
        print("ERROR: --backfill-years cannot be combined with --start-date, --end-date, or --monthly.")
        sys.exit(1)

    # --monthly: resolve to absolute date range
    if args.monthly:
        today = datetime.now()
        last_day_prev = today.replace(day=1) - timedelta(days=1)
        first_day_prev = last_day_prev.replace(day=1)
        args.start_date = first_day_prev.strftime("%Y%m%d")
        args.end_date = last_day_prev.strftime("%Y%m%d")

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    end_date = args.end_date or yesterday

    # Compute interval_days from explicit date range if provided
    if args.start_date and args.end_date:
        d0 = datetime.strptime(args.start_date, "%Y%m%d")
        d1 = datetime.strptime(args.end_date, "%Y%m%d")
        interval_days = max(1, (d1 - d0).days + 1)
    else:
        interval_days = args.interval_days

    # --estimate-only: print projection and exit
    if args.estimate_only:
        num_stores = args.num_stores or 1
        # For backfill estimate, show per-chunk and total
        if args.backfill_years:
            chunks = _backfill_chunks(args.backfill_years, args.chunk_size)
            chunk_days = max(1, (
                datetime.strptime(chunks[0][1], "%Y%m%d") -
                datetime.strptime(chunks[0][0], "%Y%m%d")
            ).days + 1) if chunks else interval_days
            print(f"\n  Backfill: {args.backfill_years} year(s), {len(chunks)} {args.chunk_size} chunk(s)\n")
            total_bytes = 0
            for s, e in chunks:
                days = (datetime.strptime(e, "%Y%m%d") - datetime.strptime(s, "%Y%m%d")).days + 1
                sizes = estimate_sizes(num_stores, days, args.orders_per_day, args.format)
                total_bytes += sizes["total"]
                print(f"  {s} → {e}  ({days}d)  est. {_human(sizes['total'])}")
            print(f"\n  TOTAL across all chunks: {_human(total_bytes)}")
            print(f"  (raw CSV equivalent: {_human(int(total_bytes * (PARQUET_COMPRESSION_RATIO if args.format == 'parquet' else 1)))})")
        else:
            print_size_estimate(
                num_stores=num_stores,
                interval_days=interval_days,
                orders_per_day=args.orders_per_day,
                fmt=args.format,
            )
        if not args.num_stores:
            print("\n  Tip: use --num-stores N to estimate for your specific store count.")
        sys.exit(0)

    # --- Initialize API client ---
    try:
        client = ToastAPIClient()
    except Exception as e:
        print(f"ERROR initializing Toast client: {e}")
        sys.exit(1)

    # --- Discover restaurants ---
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
    else:
        restaurants = all_restaurants

    num_stores = len(restaurants)
    print(f"Restaurants to process: {num_stores}")

    if args.discover:
        # Optional: load canonical store table for address→name matching
        try:
            _alex_dir = str(Path(__file__).resolve().parent.parent / "alex")
            if _alex_dir not in sys.path:
                sys.path.insert(0, _alex_dir)
            from store_locations import KNOWN_STORES as _KNOWN_STORES, match_by_address as _match_addr
        except ImportError:
            _KNOWN_STORES = []
            _match_addr = None

        total_known = len(_KNOWN_STORES) if _KNOWN_STORES else "?"
        print(f"\n{'#':<4}  {'Location':<24}  {'GUID':<38}  Toast Address")
        print("-" * 108)
        matched_guids = set()
        for i, r in enumerate(restaurants, 1):
            guid      = r.get("restaurantGuid") or r.get("guid", "")
            toast_addr = r.get("restaurantName") or r.get("name", "")
            store     = _match_addr(toast_addr) if _match_addr else None
            loc_name  = store["location_name"] if store else (r.get("locationName", "").strip() or toast_addr[:24])
            print(f"{i:<4}  {loc_name:<24}  {guid:<38}  {toast_addr}")
            matched_guids.add(guid)

        if _KNOWN_STORES:
            missing = [s for s in _KNOWN_STORES if not s["guid"]]
            if missing:
                print(f"\n  {len(missing)} store(s) not yet in this credential set:")
                for s in missing:
                    print(f"       {s['location_name']:<24}  {s['address']}")

        print(f"\nTotal accessible: {len(restaurants)} / {total_known} restaurant(s)")
        sys.exit(0)

    # --- Determine date range for BigQuery-mode (needs import_log) ---
    # For local mode with an explicit start date, we use it directly.
    # For local mode without one, fall back to interval_days from yesterday.
    if args.local:
        if args.start_date:
            start_date = args.start_date
        else:
            start_dt = datetime.now() - timedelta(days=interval_days)
            start_date = start_dt.strftime("%Y%m%d")

        output_dir = Path(args.output_dir)
        print(f"\nOutput:  local files → {output_dir.resolve()}")
        print(f"Format:  .{args.format}")
        print(f"Range:   {start_date} → {end_date}")

        # Print size estimate as a heads-up
        print_size_estimate(num_stores, interval_days, args.orders_per_day, args.format)

        if not args.dry_run:
            output_dir.mkdir(parents=True, exist_ok=True)

    else:
        # BigQuery mode — import the manager only when needed
        try:
            from database.bigquery import BigQueryManager
            bq = BigQueryManager()
            bq.create_schema()
        except Exception as e:
            print(f"ERROR initializing BigQuery: {e}")
            sys.exit(1)
        print(f"\nOutput:  BigQuery")

    print(f"Mode:    {'DRY RUN' if args.dry_run else 'LIVE'}\n")

    # --- Build list of date chunks to process ---
    if args.backfill_years:
        chunks = _backfill_chunks(args.backfill_years, args.chunk_size)
        print(f"Backfill: {args.backfill_years} year(s) | "
              f"{len(chunks)} {args.chunk_size} chunk(s) | oldest-first\n")
    else:
        # Single range — wrap in a one-item list so the loop is uniform
        chunks = [(start_date, end_date)]

    # --- Chunk loop ---
    grand_totals = {"orders": 0, "order_items": 0, "payments": 0, "customer_orders": 0, "menus": 0}
    chunk_errors = 0

    for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks):
        if args.backfill_years:
            print(f"\n{'─' * 72}")
            print(f"  CHUNK {chunk_idx + 1}/{len(chunks)}: {chunk_start} → {chunk_end}")
            print(f"{'─' * 72}")

        all_stats: List[Dict] = []

        for i, restaurant in enumerate(restaurants):
            guid = restaurant.get("restaurantGuid", restaurant.get("guid", ""))
            name = restaurant.get("restaurantName", restaurant.get("name", guid))
            logger.info(f"[{i + 1}/{num_stores}] {name}")

            if args.local:
                stats = pull_restaurant_local(
                    client=client,
                    restaurant_guid=guid,
                    restaurant_name=name,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    output_dir=output_dir,
                    fmt=args.format,
                    dry_run=args.dry_run,
                    customer_only=args.customer_only,
                )
            else:
                from toast_api.scheduler import pull_restaurant
                stats = pull_restaurant(
                    client=client,
                    bq=bq,
                    restaurant_guid=guid,
                    restaurant_name=name,
                    interval_days=interval_days,
                    dry_run=args.dry_run,
                    start_date_override=chunk_start,
                    end_date_override=chunk_end,
                    customer_only=args.customer_only,
                )

            all_stats.append(stats)

            if i < num_stores - 1 and stats.get("status") not in ("skipped", "dry_run"):
                time.sleep(DELAY_BETWEEN_RESTAURANTS)

        # --- Per-chunk summary ---
        print(f"\n{'=' * 72}")
        if args.backfill_years:
            print(f"CHUNK SUMMARY  {chunk_start} → {chunk_end}")
        else:
            print("PULL SUMMARY")
        print(f"{'=' * 72}")
        print(
            f"  {'Restaurant':<30} {'Status':<10} {'Date Range':<22}"
            f" {'Orders':>7} {'Items':>7} {'Pmts':>6} {'Cust':>6} {'Menu':>6}"
        )
        print(f"  {'-'*30} {'-'*10} {'-'*22} {'-'*7} {'-'*7} {'-'*6} {'-'*6} {'-'*6}")

        chunk_totals = {"orders": 0, "order_items": 0, "payments": 0, "customer_orders": 0, "menus": 0}
        for s in all_stats:
            date_range_str = s.get("date_range") or "up to date"
            print(
                f"  {s['restaurant'][:29]:<29} {s['status']:<10} {date_range_str:<22}"
                f" {s['orders']:>7} {s['order_items']:>7} {s['payments']:>6}"
                f" {s['customer_orders']:>6} {s['menus']:>6}"
            )
            for k in chunk_totals:
                chunk_totals[k] += s.get(k, 0)

        print(f"  {'-'*30} {'-'*10} {'-'*22} {'-'*7} {'-'*7} {'-'*6} {'-'*6} {'-'*6}")
        print(
            f"  {'TOTAL':<29} {'':<10} {'':<22}"
            f" {chunk_totals['orders']:>7} {chunk_totals['order_items']:>7}"
            f" {chunk_totals['payments']:>6} {chunk_totals['customer_orders']:>6}"
            f" {chunk_totals['menus']:>6}"
        )

        success = sum(1 for s in all_stats if s["status"] == "success")
        skipped = sum(1 for s in all_stats if s["status"] == "skipped")
        errors  = sum(1 for s in all_stats if s["status"] == "error")
        chunk_errors += errors
        print(f"\n  Results: {success} succeeded  |  {skipped} skipped  |  {errors} errors")

        for k in grand_totals:
            grand_totals[k] += chunk_totals[k]

    # --- Grand total (backfill mode only) ---
    if args.backfill_years and len(chunks) > 1:
        print(f"\n{'=' * 72}")
        print(f"BACKFILL COMPLETE — {len(chunks)} chunks")
        print(f"{'=' * 72}")
        print(
            f"  Total rows: "
            f"orders={grand_totals['orders']:,}  "
            f"items={grand_totals['order_items']:,}  "
            f"payments={grand_totals['payments']:,}  "
            f"customers={grand_totals['customer_orders']:,}  "
            f"menus={grand_totals['menus']:,}"
        )
        if chunk_errors:
            print(f"  WARNING: {chunk_errors} chunk(s) had errors — re-run to retry failed ranges.")
        print(f"{'=' * 72}")

    if args.local and not args.dry_run:
        ext = args.format
        total_actual = sum(f.stat().st_size for f in output_dir.glob(f"*.{ext}"))
        print(f"\n  Actual files written to: {output_dir.resolve()}")
        print(f"  Total disk usage: {_human(total_actual)}")

    print(f"{'=' * 72}")


if __name__ == "__main__":
    main()
