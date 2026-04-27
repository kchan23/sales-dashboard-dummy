"""
Demo database manager — reads from local synthetic parquet files.

Drop-in replacement for BigQueryManager when DEMO_MODE=true.
Implements the same public interface but operates on pre-generated synthetic
data in demo_data/*.parquet. No GCP credentials or live warehouse access.

Generate the parquet files with:
    python3 scripts/generate_demo_data.py
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent / "demo_data"


class DemoDBManager:
    """Serves analytics data from pre-generated synthetic parquet files."""

    dataset_ref = "demo_local"
    client = None

    def __init__(self):
        self._orders = pd.read_parquet(_DATA_DIR / "orders_clean.parquet")
        self._items = pd.read_parquet(_DATA_DIR / "order_items_clean.parquet")
        self._inventory = pd.read_parquet(_DATA_DIR / "inventory.parquet")
        self._reviews = pd.read_parquet(_DATA_DIR / "reviews.parquet")
        self._labor = pd.read_parquet(_DATA_DIR / "time_entries.parquet")
        self._customers = pd.read_parquet(_DATA_DIR / "customer_orders_masked.parquet")
        logger.info(
            "DemoDBManager loaded synthetic data from %s — %d orders, %d items",
            _DATA_DIR,
            len(self._orders),
            len(self._items),
        )

    # ---- Lifecycle no-ops --------------------------------------------------

    def migrate_schema(self):
        pass

    def create_schema(self):
        pass

    # ---- Location / Date Helpers -------------------------------------------

    def get_locations(self) -> List[str]:
        return sorted(self._orders["location_id"].dropna().unique().tolist())

    def get_available_dates(self, location_ids: List[str]) -> List[str]:
        df = self._orders[self._orders["location_id"].isin(location_ids)]
        return sorted(df["business_date"].dropna().unique(), reverse=True)

    # ---- Analytics Methods -------------------------------------------------

    def get_sales_summary(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        df = self._orders[
            self._orders["location_id"].isin(location_ids)
            & (self._orders["business_date"] >= start_date)
            & (self._orders["business_date"] <= end_date)
        ]
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "date", "orders", "revenue", "avg_order_value",
                    "tips", "discounts", "tax_amount",
                    "delivery_orders", "dine_in_orders", "takeout_orders",
                ]
            )

        agg = (
            df.groupby("business_date")
            .agg(
                orders=("order_guid", "count"),
                revenue=("total_amount", "sum"),
                avg_order_value=("total_amount", "mean"),
                tips=("tip_amount", "sum"),
                discounts=("discount_amount", "sum"),
                tax_amount=("tax_amount", "sum"),
            )
            .reset_index()
        )

        ot_upper = df["order_type"].fillna("").astype(str).str.upper()
        type_counts = (
            df.assign(
                delivery=(ot_upper.str.contains("DELIVERY", na=False)).astype(int),
                dine_in=(ot_upper.str.contains("DINE", na=False)).astype(int),
                takeout=(~ot_upper.str.contains("DELIVERY|DINE", na=False)).astype(int),
            )
            .groupby("business_date")[["delivery", "dine_in", "takeout"]]
            .sum()
            .rename(
                columns={
                    "delivery": "delivery_orders",
                    "dine_in": "dine_in_orders",
                    "takeout": "takeout_orders",
                }
            )
            .reset_index()
        )

        result = agg.merge(type_counts, on="business_date")
        result = result.rename(columns={"business_date": "date"})
        return result.sort_values("date")

    def get_menu_performance(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        df = self._items[
            self._items["location_id"].isin(location_ids)
            & (self._items["business_date"] >= start_date)
            & (self._items["business_date"] <= end_date)
        ]
        if df.empty:
            return pd.DataFrame(
                columns=["item", "category", "order_count", "revenue", "avg_price"]
            )

        if "unit_price" in df.columns:
            df = df.copy()
            df["_avg_price_source"] = df["unit_price"]
        elif "true_unit_price" in df.columns:
            df = df.copy()
            df["_avg_price_source"] = df["true_unit_price"]
        else:
            df = df.copy()
            df["_avg_price_source"] = df["prediscount_total"]
        result = (
            df.groupby("item_name")
            .agg(
                category=("category", "first"),
                order_count=("quantity", "sum"),
                revenue=("total_price", "sum"),
                avg_price=("_avg_price_source", "mean"),
            )
            .reset_index()
            .rename(columns={"item_name": "item"})
        )
        return result.sort_values("revenue", ascending=False)

    def get_inventory_status(
        self, location_ids: List[str], snapshot_date: str
    ) -> pd.DataFrame:
        df = self._inventory[self._inventory["location_id"].isin(location_ids)]
        if snapshot_date in df["snapshot_date"].values:
            df = df[df["snapshot_date"] == snapshot_date]
        elif not df.empty:
            df = df[df["snapshot_date"] == df["snapshot_date"].max()]

        if df.empty:
            return pd.DataFrame(
                columns=["item", "category", "stock", "reorder_level",
                         "unit_cost", "last_ordered", "status"]
            )

        return (
            df.rename(columns={"item_name": "item", "current_stock": "stock"})[
                ["item", "category", "stock", "reorder_level", "unit_cost", "last_ordered", "status"]
            ]
            .sort_values(["status", "item"])
        )

    def get_reviews(
        self,
        location_ids: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        sentiment: Optional[str] = None,
    ) -> pd.DataFrame:
        df = self._reviews[self._reviews["location_id"].isin(location_ids)]
        if start_date:
            df = df[df["review_date"] >= start_date]
        if end_date:
            df = df[df["review_date"] <= end_date]
        if sentiment and sentiment != "all":
            df = df[df["sentiment"] == sentiment]
        return df.sort_values("review_date", ascending=False)

    def get_labor_analytics(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        df = self._labor[
            self._labor["location_id"].isin(location_ids)
            & (self._labor["business_date"] >= start_date)
            & (self._labor["business_date"] <= end_date)
        ]
        return df.rename(columns={"business_date": "date"}).sort_values(
            ["date", "employee_name"], ascending=[False, True]
        )

    def get_daily_drivers_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        df = self._orders[
            (self._orders["business_date"] >= start_date)
            & (self._orders["business_date"] <= end_date)
        ]
        if df.empty:
            return pd.DataFrame()

        agg = (
            df.groupby(["location_id", "business_date"])
            .agg(
                order_count=("order_guid", "count"),
                gross_revenue=("total_amount", "sum"),
                net_revenue=("subtotal", "sum"),
                avg_order_value=("total_amount", "mean"),
                total_tips=("tip_amount", "sum"),
                total_discounts=("discount_amount", "sum"),
            )
            .reset_index()
        )

        ot_upper = df["order_type"].fillna("").astype(str).str.upper()
        type_counts = (
            df.assign(
                delivery=(ot_upper.str.contains("DELIVERY", na=False)).astype(int),
                dine_in=(ot_upper.str.contains("DINE", na=False)).astype(int),
                takeout=(~ot_upper.str.contains("DELIVERY|DINE", na=False)).astype(int),
            )
            .groupby(["location_id", "business_date"])[["delivery", "dine_in", "takeout"]]
            .sum()
            .rename(
                columns={
                    "delivery": "delivery_orders",
                    "dine_in": "dine_in_orders",
                    "takeout": "takeout_orders",
                }
            )
            .reset_index()
        )

        result = agg.merge(type_counts, on=["location_id", "business_date"])
        result["business_date"] = pd.to_datetime(
            result["business_date"], format="%Y%m%d"
        ).dt.date
        return result.sort_values(["business_date", "location_id"])

    def get_customer_analytics(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        cust = self._customers[self._customers["location_id"].isin(location_ids)]
        orders = self._orders[
            self._orders["location_id"].isin(location_ids)
            & (self._orders["business_date"] >= start_date)
            & (self._orders["business_date"] <= end_date)
        ]
        joined = cust[["customer_id", "location_id", "order_guid"]].merge(
            orders[["order_guid", "total_amount", "business_date"]],
            on="order_guid",
        )
        if joined.empty:
            return pd.DataFrame(
                columns=[
                    "customer_id", "location_id", "order_count",
                    "visit_days", "total_spend", "avg_order",
                    "first_visit", "last_visit",
                ]
            )

        return (
            joined.groupby(["customer_id", "location_id"])
            .agg(
                order_count=("order_guid", "count"),
                visit_days=("business_date", "nunique"),
                total_spend=("total_amount", "sum"),
                avg_order=("total_amount", "mean"),
                first_visit=("business_date", "min"),
                last_visit=("business_date", "max"),
            )
            .reset_index()
        )

    # ---- Methods intentionally unsupported in demo mode --------------------

    def query_to_df(self, query: str, params=None) -> pd.DataFrame:
        raise NotImplementedError("Custom SQL queries are not available in demo mode.")

    def execute(self, query: str, params=None):
        raise NotImplementedError("Custom SQL queries are not available in demo mode.")

    # ---- Write / import methods (silently ignored) -------------------------

    def log_import(self, *args, **kwargs):
        pass

    def get_latest_import_date(self, *args, **kwargs):
        return None

    def get_imported_dates(self, *args, **kwargs):
        return []

    def stream_rows(self, *args, **kwargs):
        return 0
