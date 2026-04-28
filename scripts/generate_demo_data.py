#!/usr/bin/env python3
"""
Generate synthetic demo data for the public presentation app.
Creates realistic-looking restaurant data without any real customer information.

Run from project root:
    python scripts/generate_demo_data.py
"""
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
rng = np.random.default_rng(SEED)
random.seed(SEED)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "demo_data"
OUTPUT_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOCATIONS = ["demo_downtown", "demo_westside"]
LOCATION_ORDER_BASE = {"demo_downtown": 58, "demo_westside": 42}  # avg orders/day
START = date(2025, 9, 1)
END = date(2026, 3, 22)

MENU = [
    # (name, category, unit_price)
    ("Steamed Dumplings (6pc)", "Dumplings", 9.50),
    ("Pan-Fried Dumplings (6pc)", "Dumplings", 10.00),
    ("Crispy Dumplings (6pc)", "Dumplings", 10.50),
    ("Soup Dumplings (8pc)", "Dumplings", 13.00),
    ("Pork Bao Bun", "Bao", 5.50),
    ("Chicken Bao Bun", "Bao", 5.50),
    ("Veggie Bao Bun", "Bao", 5.00),
    ("Noodle Bowl - Beef", "Noodles", 14.50),
    ("Noodle Bowl - Chicken", "Noodles", 13.50),
    ("Dan Dan Noodles", "Noodles", 12.50),
    ("Vegetable Fried Rice", "Rice", 11.00),
    ("Chicken Fried Rice", "Rice", 12.50),
    ("Combo Fried Rice", "Rice", 13.50),
    ("Spring Rolls (3pc)", "Appetizers", 7.00),
    ("Wontons in Chili Oil", "Appetizers", 8.50),
    ("Scallion Pancake", "Appetizers", 6.50),
    ("Mango Bubble Tea", "Beverages", 6.00),
    ("Taro Bubble Tea", "Beverages", 6.00),
    ("Jasmine Milk Tea", "Beverages", 5.50),
    ("Soda", "Beverages", 3.00),
    ("Sesame Dessert Ball", "Desserts", 4.50),
    ("Mochi Ice Cream (2pc)", "Desserts", 5.00),
]

ITEM_WEIGHTS = [3, 3, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 1, 2, 2, 1, 2, 2, 1, 1, 1, 1]

ORDER_TYPES = ["Dine In", "Take Out", "Delivery"]
ORDER_TYPE_WEIGHTS = [0.40, 0.40, 0.20]
CUSTOMER_CAPTURE_RATE = 0.40

# Target shape copied from the live BigQuery dashboard join distribution.
# Values are customer counts by distinct visit days, used as weights so the
# synthetic demo data has repeat-customer behavior instead of all one-timers.
VISIT_DAY_DISTRIBUTION = [
    (1, 52553),
    (2, 2666),
    (3, 846),
    (4, 346),
    (5, 161),
    (6, 110),
    (7, 67),
    (8, 40),
    (9, 28),
    (10, 25),
    (11, 17),
    (12, 13),
    (13, 12),
    (14, 7),
    (15, 3),
    (16, 7),
    (17, 2),
    (18, 2),
    (20, 2),
    (22, 1),
    (24, 3),
    (26, 1),
    (27, 1),
    (28, 1),
    (29, 2),
    (35, 1),
    (38, 1),
    (44, 1),
    (45, 1),
    (59, 1),
    (124, 1),
]
CUSTOMER_RECORDS_PER_CUSTOMER_TARGET = 1.1676504690629281

HOURS = list(range(11, 23))  # 11 am – 10 pm
HOUR_WEIGHTS = np.array([0.3, 1.5, 3.0, 2.5, 1.5, 1.0, 1.5, 3.5, 4.0, 2.5, 1.0, 0.7])

EMPLOYEES = {
    "demo_downtown": [
        ("Employee 01", "Server", 18.00),
        ("Employee 02", "Server", 17.50),
        ("Employee 03", "Cook", 20.00),
        ("Employee 04", "Cook", 19.50),
        ("Employee 05", "Manager", 28.00),
        ("Employee 06", "Server", 17.00),
    ],
    "demo_westside": [
        ("Employee 07", "Server", 17.50),
        ("Employee 08", "Server", 18.00),
        ("Employee 09", "Cook", 19.50),
        ("Employee 10", "Cook", 20.00),
        ("Employee 11", "Manager", 27.00),
        ("Employee 12", "Server", 17.00),
    ],
}

SENTIMENTS = ["positive", "neutral", "negative"]
SENTIMENT_WEIGHTS = [0.65, 0.25, 0.10]

REVIEW_TEMPLATES = {
    "positive": [
        "Really loved the {item}! Will definitely come back.",
        "Best dumplings in the city. The {item} was amazing.",
        "Fantastic experience! The {item} was perfectly cooked.",
        "Great food, friendly staff. The {item} is a must-try.",
        "Delicious as always. The {item} never disappoints.",
        "Amazing flavors! The {item} was outstanding.",
    ],
    "neutral": [
        "Food was decent. The {item} was okay.",
        "Average experience. {item} was fine, nothing special.",
        "It's alright. The {item} could use a bit more seasoning.",
        "Service was a bit slow but the {item} was good.",
        "Visited on a busy night, {item} was acceptable.",
    ],
    "negative": [
        "Disappointed with the {item}, expected better.",
        "Long wait time and the {item} was cold.",
        "Not great this time. The {item} was undercooked.",
        "Will not return. The {item} was not fresh.",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def date_multiplier(d: date) -> float:
    """Traffic multiplier based on weekday and season."""
    weekday_factor = {0: 0.75, 1: 0.80, 2: 0.85, 3: 0.90, 4: 1.10, 5: 1.25, 6: 1.15}[d.weekday()]
    seasonal = {9: 1.00, 10: 1.05, 11: 1.10, 12: 1.15, 1: 1.10, 2: 1.05, 3: 1.00}.get(d.month, 1.00)
    return weekday_factor * seasonal


def target_visit_days(record_count):
    """Allocate synthetic customers using the live repeat-visit distribution."""
    target_customers = max(1, int(round(record_count / CUSTOMER_RECORDS_PER_CUSTOMER_TARGET)))
    total_weight = sum(weight for _, weight in VISIT_DAY_DISTRIBUTION)
    expected = [
        (visit_days, target_customers * weight / total_weight)
        for visit_days, weight in VISIT_DAY_DISTRIBUTION
    ]
    counts = {visit_days: int(np.floor(count)) for visit_days, count in expected}
    remaining = target_customers - sum(counts.values())
    by_remainder = sorted(
        expected,
        key=lambda item: item[1] - np.floor(item[1]),
        reverse=True,
    )
    for visit_days, _ in by_remainder[:remaining]:
        counts[visit_days] += 1

    visit_days = [
        visit_day_count
        for visit_day_count, customer_count in counts.items()
        for _ in range(customer_count)
    ]
    while sum(visit_days) > record_count:
        idx = max(range(len(visit_days)), key=visit_days.__getitem__)
        visit_days[idx] -= 1
    return [count for count in visit_days if count > 0]


def assign_customer_ids(customer_map):
    """Reuse synthetic customer IDs across dates so visit frequency is meaningful."""
    for loc in LOCATIONS:
        loc_indices = [idx for idx, row in enumerate(customer_map) if row["location_id"] == loc]
        records_by_date = {}
        for idx in loc_indices:
            records_by_date.setdefault(customer_map[idx]["business_date"], []).append(idx)

        customer_ids_by_date = {}
        customer_counter = 1
        planned_visit_days = sorted(target_visit_days(len(loc_indices)), reverse=True)

        for visit_day_count in planned_visit_days:
            available_dates = [day for day, indices in records_by_date.items() if indices]
            if not available_dates:
                break

            selected_dates = rng.choice(
                available_dates,
                size=min(visit_day_count, len(available_dates)),
                replace=False,
            )
            customer_id = f"{loc}_cust_{customer_counter:06d}"
            customer_counter += 1

            for business_date in selected_dates:
                business_date = str(business_date)
                row_idx = records_by_date[business_date].pop()
                customer_map[row_idx]["customer_id"] = customer_id
                customer_ids_by_date.setdefault(business_date, []).append(customer_id)

        for business_date, indices in records_by_date.items():
            while indices:
                if customer_ids_by_date.get(business_date):
                    customer_id = str(rng.choice(customer_ids_by_date[business_date]))
                else:
                    customer_id = f"{loc}_cust_{customer_counter:06d}"
                    customer_counter += 1
                    customer_ids_by_date.setdefault(business_date, []).append(customer_id)

                row_idx = indices.pop()
                customer_map[row_idx]["customer_id"] = customer_id

    return customer_map


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def generate_orders_and_items():
    orders = []
    items = []
    customer_map = []
    hw = HOUR_WEIGHTS / HOUR_WEIGHTS.sum()

    for loc in LOCATIONS:
        base = LOCATION_ORDER_BASE[loc]
        d = START
        while d <= END:
            n_orders = max(5, int(base * date_multiplier(d) + rng.normal(0, 6)))
            for _ in range(n_orders):
                og = str(uuid.uuid4())
                hour = int(rng.choice(HOURS, p=hw))
                minute = int(rng.integers(0, 60))
                order_type = random.choices(ORDER_TYPES, weights=ORDER_TYPE_WEIGHTS)[0]

                n_items = int(rng.choice([1, 2, 3, 4], p=[0.20, 0.45, 0.25, 0.10]))
                selected = random.choices(MENU, weights=ITEM_WEIGHTS, k=n_items)

                # Aggregate quantities for duplicate items
                item_counts: dict = {}
                for (name, cat, price) in selected:
                    if name in item_counts:
                        item_counts[name] = (cat, price, item_counts[name][2] + 1)
                    else:
                        item_counts[name] = (cat, price, 1)

                subtotal = 0.0
                for name, (cat, price, qty) in item_counts.items():
                    tp = round(price * qty, 2)
                    subtotal += tp
                    items.append({
                        "order_guid": og,
                        "location_id": loc,
                        "business_date": d.strftime("%Y%m%d"),
                        "item_name": name,
                        "category": cat,
                        "quantity": qty,
                        "prediscount_total": price,
                        "total_price": tp,
                    })

                subtotal = round(subtotal, 2)
                tax = round(subtotal * 0.0875, 2)
                tip_rate = float(rng.uniform(0.12, 0.22))
                tip = round(subtotal * tip_rate, 2)
                draw = float(rng.uniform(0, 1))
                discount = round(subtotal * (0.10 if draw < 0.05 else 0.05 if draw < 0.12 else 0.0), 2)
                total = round(subtotal + tax + tip - discount, 2)

                orders.append({
                    "order_guid": og,
                    "location_id": loc,
                    "business_date": d.strftime("%Y%m%d"),
                    "order_time": f"{hour:02d}:{minute:02d}:00",
                    "order_type": order_type,
                    "order_category": order_type,
                    "total_amount": total,
                    "subtotal": subtotal,
                    "tax_amount": tax,
                    "tip_amount": tip,
                    "discount_amount": discount,
                    "tip_rate": round(tip / subtotal, 4) if subtotal else 0,
                    "hour_of_day": hour,
                })

                # 40% of orders have associated customer info. IDs are assigned
                # after all records are generated so repeat visits can be modeled.
                if rng.random() < CUSTOMER_CAPTURE_RATE:
                    customer_map.append({
                        "order_guid": og,
                        "location_id": loc,
                        "business_date": d.strftime("%Y%m%d"),
                    })

            d += timedelta(days=1)

    customer_map = assign_customer_ids(customer_map)
    return pd.DataFrame(orders), pd.DataFrame(items), pd.DataFrame(customer_map)


def generate_inventory():
    rows = []
    snapshot = END.strftime("%Y%m%d")
    for loc in LOCATIONS:
        for name, cat, price in MENU:
            reorder = float(rng.integers(10, 30))
            draw = float(rng.uniform(0, 1))
            if draw < 0.08:
                stock, status = 0.0, "critical"
            elif draw < 0.20:
                stock = float(rng.uniform(1, reorder))
                status = "low"
            else:
                stock = float(rng.uniform(reorder, reorder * 3))
                status = "good"
            last_ordered = (END - timedelta(days=int(rng.integers(1, 14)))).strftime("%Y%m%d")
            rows.append({
                "location_id": loc,
                "item_name": name,
                "category": cat,
                "current_stock": round(stock, 1),
                "reorder_level": reorder,
                "unit_cost": round(price * 0.35, 2),
                "last_ordered": last_ordered,
                "snapshot_date": snapshot,
                "status": status,
            })
    return pd.DataFrame(rows)


def generate_reviews(orders_df: pd.DataFrame):
    rows = []
    sampled = orders_df.sample(frac=0.15, random_state=SEED)
    for _, row in sampled.iterrows():
        sentiment = random.choices(SENTIMENTS, weights=SENTIMENT_WEIGHTS)[0]
        item_name = random.choice([m[0] for m in MENU])
        template = random.choice(REVIEW_TEMPLATES[sentiment])
        text = template.format(item=item_name)
        rating_map = {
            "positive": int(rng.choice([4, 5], p=[0.35, 0.65])),
            "neutral": int(rng.choice([3, 4], p=[0.70, 0.30])),
            "negative": int(rng.choice([1, 2], p=[0.50, 0.50])),
        }
        rows.append({
            "review_id": str(uuid.uuid4()),
            "location_id": row["location_id"],
            "order_guid": row["order_guid"],
            "review_date": row["business_date"],
            "rating": rating_map[sentiment],
            "review_text": text,
            "sentiment": sentiment,
            "category": random.choice(["Food", "Service", "Ambiance"]),
        })
    return pd.DataFrame(rows)


def generate_time_entries():
    rows = []
    for loc in LOCATIONS:
        for name, role, wage in EMPLOYEES[loc]:
            work_prob = 5 / 7 if role == "Manager" else 4 / 7
            d = START
            while d <= END:
                if rng.random() > work_prob:
                    d += timedelta(days=1)
                    continue
                if role == "Manager":
                    clock_in_hour = 9
                    hours = float(rng.uniform(8, 9))
                elif role == "Cook":
                    clock_in_hour = int(rng.choice([10, 11]))
                    hours = float(rng.uniform(7, 9))
                else:
                    clock_in_hour = int(rng.choice([11, 12, 16, 17]))
                    hours = float(rng.uniform(4, 8))

                regular = min(hours, 8.0)
                overtime = max(0.0, hours - 8.0)
                non_cash = float(rng.uniform(10, 60)) if role == "Server" else 0.0
                cash = float(rng.uniform(5, 20)) if role == "Server" else 0.0
                clock_out_hour = min(clock_in_hour + int(hours), 23)

                rows.append({
                    "location_id": loc,
                    "business_date": d.strftime("%Y%m%d"),
                    "employee_name": name,
                    "job_title": role,
                    "clock_in_time": f"{clock_in_hour:02d}:00:00",
                    "clock_out_time": f"{clock_out_hour:02d}:00:00",
                    "total_hours": round(hours, 2),
                    "payable_hours": round(hours, 2),
                    "regular_hours": round(regular, 2),
                    "overtime_hours": round(overtime, 2),
                    "cash_tips": round(cash, 2),
                    "non_cash_tips": round(non_cash, 2),
                    "total_gratuity": round(non_cash, 2),
                    "total_tips": round(non_cash + cash, 2),
                    "wage": wage,
                })
                d += timedelta(days=1)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Generating synthetic demo data...")

    print("  → orders + items + customer map...")
    orders_df, items_df, customers_df = generate_orders_and_items()
    print(f"     {len(orders_df):,} orders  |  {len(items_df):,} line items  |  {len(customers_df):,} customer records")

    print("  → inventory...")
    inventory_df = generate_inventory()

    print("  → reviews...")
    reviews_df = generate_reviews(orders_df)

    print("  → time entries...")
    labor_df = generate_time_entries()

    orders_df.to_parquet(OUTPUT_DIR / "orders_clean.parquet", index=False)
    items_df.to_parquet(OUTPUT_DIR / "order_items_clean.parquet", index=False)
    customers_df.to_parquet(OUTPUT_DIR / "customer_orders_masked.parquet", index=False)
    inventory_df.to_parquet(OUTPUT_DIR / "inventory.parquet", index=False)
    reviews_df.to_parquet(OUTPUT_DIR / "reviews.parquet", index=False)
    labor_df.to_parquet(OUTPUT_DIR / "time_entries.parquet", index=False)

    print(f"\nDone — files written to {OUTPUT_DIR}/")
    for f in sorted(OUTPUT_DIR.glob("*.parquet")):
        print(f"  {f.name}: {f.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
