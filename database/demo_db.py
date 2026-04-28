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
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent / "demo_data"


class DemoQueryJob:
    """Small adapter matching BigQuery QueryJob's to_dataframe interface."""

    def __init__(self, df: pd.DataFrame):
        self._df = df

    def to_dataframe(self) -> pd.DataFrame:
        return self._df.copy()


class DemoDBManager:
    """Serves analytics data from pre-generated synthetic parquet files."""

    dataset_ref = "demo_local"
    client = None
    is_demo = True

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

    # ---- Demo SQL support --------------------------------------------------

    def query_to_df(self, query: str, params=None) -> pd.DataFrame:
        return self.execute(query, params).to_dataframe()

    def execute(self, query: str, params=None):
        marker = self._demo_query_marker(query)
        if not marker:
            raise NotImplementedError(
                "Only generated demo queries are available in demo mode."
            )

        param_map = self._param_map(params)
        location_id = param_map.get("location_id")
        location_ids = [location_id] if location_id else self.get_locations()
        start_date = param_map.get("start_date", self._orders["business_date"].min())
        end_date = param_map.get("end_date", self._orders["business_date"].max())
        snapshot_date = param_map.get("snapshot_date", end_date)

        if marker in {"daily_revenue", "average_order_value"}:
            df = self.get_sales_summary(location_ids, start_date, end_date)[
                ["date", "orders", "revenue", "avg_order_value"]
            ]
        elif marker == "sales_summary":
            daily = self.get_sales_summary(location_ids, start_date, end_date)
            orders = daily["orders"].sum() if not daily.empty else 0
            revenue = daily["revenue"].sum() if not daily.empty else 0
            df = pd.DataFrame([{
                "orders": orders,
                "revenue": revenue,
                "avg_order_value": revenue / orders if orders else 0,
                "tips": daily["tips"].sum() if not daily.empty else 0,
                "discounts": daily["discounts"].sum() if not daily.empty else 0,
            }])
        elif marker in {"top_items_by_revenue", "top_items_by_orders"}:
            df = self.get_menu_performance(location_ids, start_date, end_date)
            sort_col = "revenue" if marker == "top_items_by_revenue" else "order_count"
            df = df.sort_values(sort_col, ascending=False).head(10)
        elif marker == "category_performance":
            items = self._filter_items(location_ids, start_date, end_date)
            if items.empty:
                df = pd.DataFrame(columns=["category", "order_count", "revenue", "avg_price"])
            else:
                price_col = self._item_price_column(items)
                df = (
                    items.groupby("category")
                    .agg(
                        order_count=("quantity", "sum"),
                        revenue=("total_price", "sum"),
                        avg_price=(price_col, "mean"),
                    )
                    .reset_index()
                    .sort_values("revenue", ascending=False)
                )
        elif marker == "category_revenue_mix":
            items = self._filter_items(location_ids, start_date, end_date)
            if items.empty:
                df = pd.DataFrame(
                    columns=[
                        "category", "order_count", "revenue",
                        "revenue_share_pct", "avg_price", "revenue_rank",
                    ]
                )
            else:
                price_col = self._item_price_column(items)
                df = (
                    items.groupby("category")
                    .agg(
                        order_count=("quantity", "sum"),
                        revenue=("total_price", "sum"),
                        avg_price=(price_col, "mean"),
                    )
                    .reset_index()
                    .sort_values("revenue", ascending=False)
                )
                total_revenue = df["revenue"].sum()
                df["revenue_share_pct"] = (
                    df["revenue"].div(total_revenue).mul(100).round(2)
                    if total_revenue
                    else 0
                )
                df["revenue_rank"] = range(1, len(df) + 1)
                df = df[
                    [
                        "category", "order_count", "revenue",
                        "revenue_share_pct", "avg_price", "revenue_rank",
                    ]
                ]
        elif marker == "order_type_mix":
            orders = self._filter_orders(location_ids, start_date, end_date)
            if orders.empty:
                df = pd.DataFrame(columns=["order_type", "orders", "revenue", "avg_order_value"])
            else:
                df = (
                    orders.groupby("order_type")
                    .agg(
                        orders=("order_guid", "count"),
                        revenue=("total_amount", "sum"),
                        avg_order_value=("total_amount", "mean"),
                    )
                    .reset_index()
                    .sort_values("revenue", ascending=False)
                )
        elif marker == "inventory_attention":
            df = self.get_inventory_status(location_ids, snapshot_date)
            df = df[df["status"].isin(["low", "critical"])][
                ["item", "category", "stock", "reorder_level", "status"]
            ]
        elif marker == "review_sentiment":
            reviews = self.get_reviews(location_ids, start_date, end_date)
            if reviews.empty:
                df = pd.DataFrame(columns=["sentiment", "review_count", "avg_rating"])
            else:
                df = (
                    reviews.groupby("sentiment")
                    .agg(review_count=("rating", "count"), avg_rating=("rating", "mean"))
                    .reset_index()
                    .sort_values("review_count", ascending=False)
                )
        elif marker == "customer_summary":
            customers = self.get_customer_analytics(location_ids, start_date, end_date)
            if customers.empty:
                df = pd.DataFrame(
                    columns=["customer_segment", "customers", "avg_total_spend", "avg_order_value"]
                )
            else:
                df = customers.copy()
                df["customer_segment"] = pd.cut(
                    df["order_count"],
                    bins=[0, 1, 4, float("inf")],
                    labels=["1 order", "2-4 orders", "5+ orders"],
                    include_lowest=True,
                )
                df = (
                    df.groupby("customer_segment", observed=True)
                    .agg(
                        customers=("customer_id", "count"),
                        avg_total_spend=("total_spend", "mean"),
                        avg_order_value=("avg_order", "mean"),
                    )
                    .reset_index()
                    .sort_values("customers", ascending=False)
                )
        else:
            raise NotImplementedError(f"Unsupported demo query: {marker}")

        return DemoQueryJob(df.reset_index(drop=True))

    def _filter_orders(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        return self._orders[
            self._orders["location_id"].isin(location_ids)
            & (self._orders["business_date"] >= start_date)
            & (self._orders["business_date"] <= end_date)
        ]

    def _filter_items(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        return self._items[
            self._items["location_id"].isin(location_ids)
            & (self._items["business_date"] >= start_date)
            & (self._items["business_date"] <= end_date)
        ]

    def _item_price_column(self, items: pd.DataFrame) -> str:
        if "true_unit_price" in items.columns:
            return "true_unit_price"
        if "unit_price" in items.columns:
            return "unit_price"
        if "prediscount_total" in items.columns:
            return "prediscount_total"
        return "total_price"

    def _demo_query_marker(self, query: str) -> Optional[str]:
        match = re.search(r"/\*\s*DEMO_QUERY:\s*([a-z_]+)\s*\*/", query)
        return match.group(1) if match else None

    def _param_map(self, params) -> dict:
        result = {}
        for param in params or []:
            name = getattr(param, "name", None)
            value = getattr(param, "value", None)
            if name:
                result[name] = value
        return result

    # ---- Write / import methods (silently ignored) -------------------------

    def log_import(self, *args, **kwargs):
        pass

    def get_latest_import_date(self, *args, **kwargs):
        return None

    def get_imported_dates(self, *args, **kwargs):
        return []

    def stream_rows(self, *args, **kwargs):
        return 0
