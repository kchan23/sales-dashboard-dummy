#!/usr/bin/env python3
"""
Toast API Test Pull Script.

Authenticates with the Toast API and pulls raw data from each endpoint
you have access to, saving JSON locally for inspection.

Usage:
    # Discover all restaurants your credentials can access:
    python -m toast_api.test_pull --discover

    # Pull data for a specific restaurant:
    python -m toast_api.test_pull --restaurant-id <GUID> --start-date 2025-02-09 --end-date 2025-02-09

    # Pull only orders or menus:
    python -m toast_api.test_pull --restaurant-id <GUID> --orders-only
    python -m toast_api.test_pull --restaurant-id <GUID> --menus-only
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

from toast_api.client import ToastAPIClient
from toast_api.field_mapping import print_mapping_comparison

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "test_output"


def save_json(data, filename: str) -> Path:
    """Save data as formatted JSON to the test_output directory."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Saved {filepath} ({filepath.stat().st_size:,} bytes)")
    return filepath


def run_discover(client: ToastAPIClient):
    """Discover and list all accessible restaurants."""
    print("Discovering restaurants...")
    restaurants = client.discover_restaurants()
    save_json(restaurants, "restaurants.json")

    print(f"\n{'='*60}")
    print(f"RESTAURANTS DISCOVERED: {len(restaurants)}")
    print(f"{'='*60}")

    if not restaurants:
        print("No restaurants found. Check your API credentials and scopes.")
        return

    # Print each restaurant's key info
    print(f"\n{'GUID':<40} {'Name'}")
    print(f"{'─'*40} {'─'*30}")
    for r in restaurants:
        guid = r.get("guid", r.get("restaurantGuid", "unknown"))
        name = r.get("restaurantName", r.get("name", "unknown"))
        print(f"  {guid:<38} {name}")

    print(f"\nFull details saved to: {OUTPUT_DIR / 'restaurants.json'}")
    print(f"\nTo pull data for a restaurant, run:")
    if restaurants:
        sample_guid = restaurants[0].get("guid", restaurants[0].get("restaurantGuid", "<GUID>"))
        print(f"  python -m toast_api.test_pull --restaurant-id {sample_guid} --start-date 2025-02-09 --end-date 2025-02-09")


def summarize_orders(orders: list):
    """Print a summary of fetched orders."""
    print(f"\n{'='*60}")
    print(f"ORDERS SUMMARY")
    print(f"{'='*60}")
    print(f"Total orders fetched: {len(orders)}")

    if not orders:
        print("No orders found for this date range.")
        return

    # Show top-level keys from first order
    sample = orders[0]
    print(f"\nTop-level fields in order object:")
    for key in sorted(sample.keys()):
        val = sample[key]
        val_type = type(val).__name__
        if isinstance(val, list):
            val_preview = f"list[{len(val)} items]"
        elif isinstance(val, dict):
            val_preview = f"dict{{{', '.join(list(val.keys())[:4])}...}}"
        elif isinstance(val, str) and len(val) > 50:
            val_preview = f'"{val[:50]}..."'
        else:
            val_preview = repr(val)
        print(f"  {key}: {val_type} = {val_preview}")

    # Show check/payment/item counts
    total_checks = sum(len(o.get("checks", [])) for o in orders)
    total_payments = sum(
        len(c.get("payments", []))
        for o in orders
        for c in o.get("checks", [])
    )
    total_selections = sum(
        len(c.get("selections", []))
        for o in orders
        for c in o.get("checks", [])
    )
    print(f"\nAggregates across all orders:")
    print(f"  Checks: {total_checks}")
    print(f"  Payments: {total_payments}")
    print(f"  Item selections: {total_selections}")


def summarize_customer_data(orders: list):
    """Inspect checks[].customer fields for population rate and data presence."""
    print(f"\n{'='*60}")
    print(f"CUSTOMER DATA SUMMARY")
    print(f"{'='*60}")

    total_checks = 0
    checks_with_customer = 0
    has_email = 0
    has_phone = 0
    has_name = 0
    seen_emails: set = set()
    sample_masked = []

    for order in orders:
        for check in order.get("checks", []):
            total_checks += 1
            customer = check.get("customer")
            if not customer:
                continue
            checks_with_customer += 1
            email = (customer.get("email") or "").strip()
            phone = (customer.get("phone") or "").strip()
            first = (customer.get("firstName") or "").strip()
            last = (customer.get("lastName") or "").strip()
            if email:
                has_email += 1
                seen_emails.add(email.lower())
                if len(sample_masked) < 3:
                    local, _, domain = email.partition("@")
                    sample_masked.append(f"{local[0]}***@{domain}" if local else f"***@{domain}")
            if phone:
                has_phone += 1
            if first or last:
                has_name += 1

    print(f"Total checks inspected: {total_checks}")
    if total_checks == 0:
        return

    pct = checks_with_customer / total_checks * 100
    print(f"Checks with customer object: {checks_with_customer} ({pct:.1f}%)")
    print(f"  email populated:  {has_email}")
    print(f"  phone populated:  {has_phone}")
    print(f"  name populated:   {has_name}")
    print(f"Unique emails seen: {len(seen_emails)}")
    if sample_masked:
        print(f"Sample (masked):    {', '.join(sample_masked)}")
    elif checks_with_customer > 0:
        print("  (customer objects present but all fields empty)")
    else:
        print("\nNo customer data found — guest.pi:read scope may not be active on these credentials.")


def inspect_modifiers(orders: list):
    """Check whether selections have non-empty modifiers arrays."""
    print(f"\n{'='*60}")
    print(f"INSPECT: ITEM MODIFIERS  (selections[].modifiers[])")
    print(f"{'='*60}")
    total_selections = 0
    selections_with_modifiers = 0
    modifier_samples = []
    for order in orders:
        for check in order.get("checks", []):
            for sel in check.get("selections", []):
                if sel.get("voided"):
                    continue
                total_selections += 1
                mods = sel.get("modifiers") or []
                if mods:
                    selections_with_modifiers += 1
                    if len(modifier_samples) < 3:
                        modifier_samples.append({
                            "item": sel.get("displayName"),
                            "modifiers": [m.get("displayName") for m in mods[:4]],
                        })
    pct = selections_with_modifiers / total_selections * 100 if total_selections else 0
    print(f"Total selections:       {total_selections:,}")
    print(f"With modifiers:         {selections_with_modifiers:,} ({pct:.1f}%)")
    if modifier_samples:
        print("Samples:")
        for s in modifier_samples:
            print(f"  {s['item']} → {s['modifiers']}")
    else:
        print("NO modifier data found — modifiers may not be configured on this account.")


def inspect_dining_option(orders: list):
    """Check diningOption field population on orders."""
    print(f"\n{'='*60}")
    print(f"INSPECT: DINING OPTION  (orders[].diningOption)")
    print(f"{'='*60}")
    total = len(orders)
    populated = [o.get("diningOption") for o in orders if o.get("diningOption")]
    pct = len(populated) / total * 100 if total else 0
    print(f"Orders with diningOption: {len(populated)}/{total} ({pct:.1f}%)")
    if populated:
        # Show unique values seen
        unique_names = sorted({
            (d.get("name") or d) if isinstance(d, dict) else str(d)
            for d in populated[:50]
        })
        print(f"Unique values seen (up to 10): {unique_names[:10]}")
        print(f"First raw sample: {populated[0]}")
    else:
        print("NO diningOption data found on any order.")


def inspect_server_on_check(orders: list):
    """Check whether checks carry employee/server info."""
    print(f"\n{'='*60}")
    print(f"INSPECT: SERVER ON CHECK  (checks[].employee or .server)")
    print(f"{'='*60}")
    total_checks = 0
    with_employee = 0
    samples = []
    for order in orders:
        for check in order.get("checks", []):
            total_checks += 1
            emp = check.get("employee") or check.get("server")
            if emp:
                with_employee += 1
                if len(samples) < 3:
                    samples.append(emp)
    pct = with_employee / total_checks * 100 if total_checks else 0
    print(f"Checks with employee/server: {with_employee}/{total_checks} ({pct:.1f}%)")
    if samples:
        print("Samples:")
        for s in samples:
            print(f"  {s}")
    else:
        print("NO employee/server data found on checks.")


def inspect_labor(client, start_date: str, end_date: str):
    """Attempt several labor API endpoint paths and report HTTP status + shape."""
    print(f"\n{'='*60}")
    print(f"INSPECT: LABOR API  (requires labor:read scope)")
    print(f"{'='*60}")
    # business date without dashes for query param
    biz_date = start_date.replace("-", "")
    paths_to_try = [
        f"/labor/v1/timeEntries?businessDate={biz_date}",
        f"/labor/v2/timeEntries?businessDate={biz_date}",
        f"/labor/v1/shifts?businessDate={biz_date}",
        f"/labor/v2/shifts?businessDate={biz_date}",
    ]
    for path in paths_to_try:
        try:
            resp = client.get(path)
            status = resp.status_code
            print(f"  {path}")
            print(f"    → HTTP {status}")
            if status == 200:
                data = resp.json()
                if isinstance(data, list):
                    print(f"    → {len(data)} records returned")
                    if data:
                        print(f"    → First record keys: {list(data[0].keys())[:12]}")
                else:
                    print(f"    → Response type: {type(data).__name__}")
                return  # stop on first success
            elif status == 403:
                print(f"    → Access denied — labor:read scope not active on these credentials")
            elif status == 404:
                print(f"    → Endpoint not found — may not be available on this Toast plan")
            else:
                print(f"    → {resp.text[:200]}")
        except Exception as e:
            print(f"  {path} → ERROR: {e}")
    print("  Labor API: no endpoint returned 200 — data not available on this account.")


def summarize_menus(menus: list):
    """Print a summary of fetched menu data."""
    print(f"\n{'='*60}")
    print(f"MENUS SUMMARY")
    print(f"{'='*60}")
    print(f"Total menu objects fetched: {len(menus)}")

    if not menus:
        print("No menu data found.")
        return

    sample = menus[0]
    print(f"\nTop-level fields in menu object:")
    for key in sorted(sample.keys()):
        val = sample[key]
        val_type = type(val).__name__
        if isinstance(val, list):
            val_preview = f"list[{len(val)} items]"
        elif isinstance(val, dict):
            val_preview = f"dict{{{', '.join(list(val.keys())[:4])}...}}"
        else:
            val_preview = repr(val)[:60]
        print(f"  {key}: {val_type} = {val_preview}")

    # Count menu groups and items
    total_groups = sum(len(m.get("menuGroups", [])) for m in menus)
    total_items = sum(
        len(g.get("menuItems", []))
        for m in menus
        for g in m.get("menuGroups", [])
    )
    print(f"\nAggregates:")
    print(f"  Menu groups: {total_groups}")
    print(f"  Menu items: {total_items}")


def summarize_kitchen(stations: list):
    """Print a summary of fetched prep station data."""
    print(f"\n{'='*60}")
    print(f"KITCHEN / PREP STATIONS SUMMARY")
    print(f"{'='*60}")
    print(f"Total prep stations fetched: {len(stations)}")

    if not stations:
        print("No prep station data found.")
        return

    sample = stations[0]
    print(f"\nTop-level fields in prep station object:")
    for key in sorted(sample.keys()):
        val = sample[key]
        val_type = type(val).__name__
        val_preview = repr(val)[:60]
        print(f"  {key}: {val_type} = {val_preview}")


def run_test_access(client: ToastAPIClient):
    """Test data-level access for all restaurants in restaurants.json."""
    restaurants_file = OUTPUT_DIR / "restaurants.json"
    if not restaurants_file.exists():
        print("ERROR: restaurants.json not found. Run --discover first.")
        sys.exit(1)

    with open(restaurants_file) as f:
        restaurants = json.load(f)

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Testing data access for {len(restaurants)} restaurants (date: {yesterday})...")
    print()
    print(f"  {'Location':<28} {'GUID':<38} Result")
    print(f"  {'─'*28} {'─'*38} {'─'*20}")

    accessible = []
    denied = []

    for r in restaurants:
        guid = r.get("restaurantGuid", r.get("guid", "unknown"))
        name = r.get("locationName", r.get("restaurantName", "unknown"))[:28]

        try:
            client.set_restaurant(guid)
            orders = client.get_orders_bulk(yesterday, yesterday, page_size=1)
            result = f"✓  {len(orders)} order(s) returned"
            accessible.append(guid)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            result = f"✗  HTTP {status}"
            denied.append((guid, status))
        except Exception as e:
            result = f"✗  {type(e).__name__}: {str(e)[:30]}"
            denied.append((guid, "ERR"))

        print(f"  {name:<28} {guid:<38} {result}")
        time.sleep(2)

    print()
    print(f"{'='*70}")
    print(f"Summary: {len(accessible)}/{len(restaurants)} accessible, {len(denied)}/{len(restaurants)} denied")
    if denied:
        print(f"\nDenied GUIDs:")
        for guid, status in denied:
            print(f"  {guid}  (HTTP {status})")


def main():
    parser = argparse.ArgumentParser(description="Pull test data from Toast API")
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Discover all restaurants accessible with your credentials.",
    )
    parser.add_argument(
        "--test-access",
        action="store_true",
        help="Test data-level access for all restaurants in restaurants.json.",
    )
    parser.add_argument(
        "--restaurant-id",
        help="Restaurant external ID (GUID) to pull data for.",
    )
    parser.add_argument(
        "--start-date",
        help="Start date (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--orders-only",
        action="store_true",
        help="Only pull orders (skip menus and kitchen).",
    )
    parser.add_argument(
        "--menus-only",
        action="store_true",
        help="Only pull menus (skip orders and kitchen).",
    )
    args = parser.parse_args()

    # Validate args
    if not args.discover and not args.test_access and not args.restaurant_id:
        parser.error("Either --discover, --test-access, or --restaurant-id is required.")

    print(f"Toast API Test Pull")
    print(f"Output directory: {OUTPUT_DIR}")
    print()

    # Initialize client
    try:
        client = ToastAPIClient()
    except (ValueError, FileNotFoundError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # --- Discovery mode ---
    if args.discover:
        run_discover(client)
        return

    # --- Access test mode ---
    if args.test_access:
        run_test_access(client)
        return

    # --- Data pull mode (requires restaurant ID) ---
    client.set_restaurant(args.restaurant_id)

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = args.start_date or yesterday
    end_date = args.end_date or yesterday
    print(f"Restaurant: {args.restaurant_id}")
    print(f"Date range: {start_date} to {end_date}")
    print()

    pull_all = not args.orders_only and not args.menus_only

    # --- Pull Orders ---
    if pull_all or args.orders_only:
        print("Pulling orders...")
        try:
            orders = client.get_orders_bulk(start_date, end_date)
            save_json(orders, f"orders_{start_date}_to_{end_date}.json")
            summarize_orders(orders)
            summarize_customer_data(orders)
            inspect_modifiers(orders)
            inspect_dining_option(orders)
            inspect_server_on_check(orders)
            if orders:
                print_mapping_comparison("orders", orders[0])
                # Check guest field mapping on first check that has a customer object
                for _order in orders:
                    for _check in _order.get("checks", []):
                        if _check.get("customer"):
                            print_mapping_comparison("guest", _check)
                            break
                    else:
                        continue
                    break
        except Exception as e:
            logger.error(f"Failed to pull orders: {e}")
            print(f"ERROR pulling orders: {e}")

    # --- Pull Menus ---
    if pull_all or args.menus_only:
        print("\nPulling menus...")
        try:
            menus = client.get_menus()
            save_json(menus, "menus.json")
            summarize_menus(menus)
            if menus:
                print_mapping_comparison("menus", menus[0])
        except Exception as e:
            logger.error(f"Failed to pull menus: {e}")
            print(f"ERROR pulling menus: {e}")

    # --- Pull Kitchen / Prep Stations ---
    if pull_all:
        print("\nPulling prep stations...")
        try:
            stations = client.get_prep_stations()
            save_json(stations, "prep_stations.json")
            summarize_kitchen(stations)
        except Exception as e:
            logger.error(f"Failed to pull prep stations: {e}")
            print(f"ERROR pulling prep stations: {e}")

    # --- Labor API probe ---
    if not args.menus_only:
        inspect_labor(client, start_date, end_date)

    print(f"\n{'='*60}")
    print(f"DONE. Raw JSON saved to: {OUTPUT_DIR}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
