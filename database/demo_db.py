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

import numpy as np
import pandas as pd

from database.objective5 import confidence_label, recommended_action

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
                    "subtotal", "tips", "discounts", "tax_amount",
                    "delivery_orders", "dine_in_orders", "takeout_orders",
                    "api_orders", "other_orders", "dine_in_subtotal",
                    "dine_in_tips", "dine_in_zero_tip_orders", "api_subtotal",
                    "api_tips", "api_zero_tip_orders", "delivery_subtotal",
                    "delivery_tips", "delivery_zero_tip_orders", "takeout_subtotal",
                    "takeout_tips", "takeout_zero_tip_orders",
                ]
            )

        ot_upper = df["order_type"].fillna("").astype(str).str.upper()
        delivery_mask = ot_upper.str.contains("DELIVERY", na=False)
        takeout_mask = ot_upper.str.contains("TAKEOUT|TAKE OUT|PICKUP|PICK UP|TAKE OUT", na=False)
        api_mask = ot_upper.eq("API")
        dine_in_mask = ot_upper.str.contains("DINE|TABLE|IN STORE|ORDER-AND-PAY", na=False)
        other_mask = ~(delivery_mask | takeout_mask | api_mask | dine_in_mask)
        enriched = df.assign(
            delivery=delivery_mask.astype(int),
            dine_in=dine_in_mask.astype(int),
            takeout=takeout_mask.astype(int),
            api=api_mask.astype(int),
            other=other_mask.astype(int),
            dine_in_subtotal=df["subtotal"].where(dine_in_mask, 0),
            dine_in_tips=df["tip_amount"].where(dine_in_mask, 0),
            dine_in_zero_tip=((df["tip_amount"].fillna(0) == 0) & dine_in_mask).astype(int),
            api_subtotal=df["subtotal"].where(api_mask, 0),
            api_tips=df["tip_amount"].where(api_mask, 0),
            api_zero_tip=((df["tip_amount"].fillna(0) == 0) & api_mask).astype(int),
            delivery_subtotal=df["subtotal"].where(delivery_mask, 0),
            delivery_tips=df["tip_amount"].where(delivery_mask, 0),
            delivery_zero_tip=((df["tip_amount"].fillna(0) == 0) & delivery_mask).astype(int),
            takeout_subtotal=df["subtotal"].where(takeout_mask, 0),
            takeout_tips=df["tip_amount"].where(takeout_mask, 0),
            takeout_zero_tip=((df["tip_amount"].fillna(0) == 0) & takeout_mask).astype(int),
        )

        agg = (
            enriched.groupby("business_date")
            .agg(
                orders=("order_guid", "count"),
                revenue=("total_amount", "sum"),
                subtotal=("subtotal", "sum"),
                avg_order_value=("total_amount", "mean"),
                tips=("tip_amount", "sum"),
                discounts=("discount_amount", "sum"),
                tax_amount=("tax_amount", "sum"),
                delivery_orders=("delivery", "sum"),
                dine_in_orders=("dine_in", "sum"),
                takeout_orders=("takeout", "sum"),
                api_orders=("api", "sum"),
                other_orders=("other", "sum"),
                dine_in_subtotal=("dine_in_subtotal", "sum"),
                dine_in_tips=("dine_in_tips", "sum"),
                dine_in_zero_tip_orders=("dine_in_zero_tip", "sum"),
                api_subtotal=("api_subtotal", "sum"),
                api_tips=("api_tips", "sum"),
                api_zero_tip_orders=("api_zero_tip", "sum"),
                delivery_subtotal=("delivery_subtotal", "sum"),
                delivery_tips=("delivery_tips", "sum"),
                delivery_zero_tip_orders=("delivery_zero_tip", "sum"),
                takeout_subtotal=("takeout_subtotal", "sum"),
                takeout_tips=("takeout_tips", "sum"),
                takeout_zero_tip_orders=("takeout_zero_tip", "sum"),
            )
            .reset_index()
        )
        return agg.rename(columns={"business_date": "date"}).sort_values("date")

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

    def _get_obj5_gold(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        items = self._filter_items(location_ids, start_date, end_date).copy()
        orders = self._filter_orders(location_ids, start_date, end_date).copy()
        if items.empty or orders.empty:
            return pd.DataFrame()

        items["canonical_name"] = items.get("canonical_name", items["item_name"])
        items["display_name"] = items.get("display_name", items["item_name"])
        if "unit_price" not in items.columns:
            price_col = self._item_price_column(items)
            if price_col in {"prediscount_total", "total_price"}:
                items["unit_price"] = items[price_col] / items["quantity"].replace(0, pd.NA)
            else:
                items["unit_price"] = items[price_col]

        order_cols = [
            "order_guid", "location_id", "business_date", "discount_amount",
            "total_amount", "order_type", "hour_of_day",
        ]
        available = [col for col in order_cols if col in orders.columns]
        gold = items.merge(
            orders[available].drop_duplicates(["order_guid", "location_id", "business_date"]),
            on=["order_guid", "location_id", "business_date"],
            how="inner",
            suffixes=("", "_order"),
        )
        if gold.empty:
            return gold

        gold["discount_amount"] = gold.get("discount_amount", 0).fillna(0)
        order_line_revenue = gold.groupby("order_guid")["total_price"].transform("sum")
        gold["alloc_discount"] = np.where(
            order_line_revenue > 0,
            gold["discount_amount"] * gold["total_price"].fillna(0) / order_line_revenue,
            0,
        )
        gold["net_line_revenue"] = gold["total_price"].fillna(0) - gold["alloc_discount"]

        inv_cols = [c for c in ["location_id", "item_name", "category", "unit_cost"] if c in self._inventory.columns]
        if {"location_id", "item_name", "unit_cost"}.issubset(inv_cols):
            cost_map = (
                self._inventory[inv_cols]
                .dropna(subset=["item_name"])
                .groupby(["location_id", "item_name"], as_index=False)
                .agg(
                    unit_cost=("unit_cost", "mean"),
                    category_inventory=("category", "first") if "category" in inv_cols else ("item_name", "first"),
                )
            )
            gold = gold.merge(cost_map, on=["location_id", "item_name"], how="left")
        else:
            gold["unit_cost"] = np.nan
            gold["category_inventory"] = np.nan

        gold["category_filled"] = (
            gold.get("category", pd.Series(index=gold.index, dtype="object"))
            .replace("", pd.NA)
            .fillna(gold.get("category_inventory", pd.Series(index=gold.index, dtype="object")))
            .fillna("UNKNOWN")
        )
        gold["est_cogs"] = gold["quantity"].fillna(0) * gold["unit_cost"].fillna(0)
        gold["est_margin"] = np.where(
            gold["unit_cost"].notna(),
            gold["net_line_revenue"] - gold["est_cogs"],
            np.nan,
        )
        gold["is_discounted"] = gold["discount_amount"] > 0
        gold["is_drink"] = gold["category_filled"].astype(str).str.upper().isin(
            {"BEVERAGES", "ALCOHOL", "DRINKS", "BEER", "WINE", "SPIRITS", "COCKTAILS"}
        )
        return gold

    def _get_obj5_kpis(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        gold = self._get_obj5_gold(location_ids, start_date, end_date)
        if gold.empty:
            return pd.DataFrame()

        total_orders = gold["order_guid"].nunique()

        def _weighted_unit_revenue(s):
            qty = gold.loc[s.index, "quantity"].sum()
            return s.sum() / qty if qty else np.nan

        def _weighted_margin(s):
            qty = gold.loc[s.index, "quantity"].sum()
            return s.sum() / qty if qty and s.notna().any() else np.nan

        kpis = (
            gold.groupby("canonical_name")
            .agg(
                display_name=("display_name", "first"),
                category=("category_filled", "first"),
                total_qty=("quantity", "sum"),
                orders_with_item=("order_guid", "nunique"),
                net_revenue=("net_line_revenue", "sum"),
                avg_unit_price=("unit_price", "mean"),
                avg_net_rev_per_unit=("net_line_revenue", _weighted_unit_revenue),
                avg_est_margin_per_unit=("est_margin", _weighted_margin),
                est_margin=("est_margin", "sum"),
                cost_qty=("unit_cost", lambda s: gold.loc[s.index, "quantity"][s.notna()].sum()),
                discount_dollars=("alloc_discount", "sum"),
            )
            .reset_index()
        )
        kpis["popularity"] = kpis["orders_with_item"] / total_orders if total_orders else 0
        kpis["avg_net_rev_per_unit"] = kpis["avg_net_rev_per_unit"].replace([np.inf, -np.inf], np.nan)
        kpis["avg_est_margin_per_unit"] = kpis["avg_est_margin_per_unit"].replace([np.inf, -np.inf], np.nan)
        kpis["cost_coverage"] = kpis["cost_qty"] / kpis["total_qty"].replace(0, np.nan)
        kpis["est_margin_pct"] = kpis["est_margin"] / kpis["net_revenue"].replace(0, np.nan)
        kpis["discount_rate"] = kpis["discount_dollars"] / (
            kpis["net_revenue"] + kpis["discount_dollars"]
        ).replace(0, np.nan)
        kpis["margin_axis"] = kpis["avg_est_margin_per_unit"].fillna(kpis["avg_net_rev_per_unit"])
        kpis["margin_source"] = np.where(
            kpis["cost_coverage"].fillna(0) > 0,
            "Inventory cost",
            "Revenue/unit proxy",
        )

        pop_threshold = kpis["popularity"].median()
        margin_threshold = kpis["margin_axis"].median()
        kpis["quadrant"] = np.select(
            [
                (kpis["popularity"] >= pop_threshold) & (kpis["margin_axis"] >= margin_threshold),
                (kpis["popularity"] >= pop_threshold) & (kpis["margin_axis"] < margin_threshold),
                (kpis["popularity"] < pop_threshold) & (kpis["margin_axis"] >= margin_threshold),
            ],
            ["Star", "Plowhorse", "Puzzle"],
            default="Dog",
        )
        return kpis.sort_values("net_revenue", ascending=False)

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

    def get_menu_engineering(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        kpis = self._get_obj5_kpis(location_ids, start_date, end_date)
        if kpis.empty:
            return pd.DataFrame(
                columns=["item", "category", "order_count", "revenue", "avg_price", "quadrant"]
            )

        return (
            kpis.rename(
                columns={
                    "display_name": "item",
                    "orders_with_item": "order_count",
                    "net_revenue": "revenue",
                    "avg_unit_price": "avg_price",
                }
            )[["item", "category", "order_count", "revenue", "avg_price", "quadrant"]]
            .sort_values("revenue", ascending=False)
        )

    def get_menu_recommendations(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        kpis = self._get_obj5_kpis(location_ids, start_date, end_date)
        if kpis.empty:
            return pd.DataFrame(
                columns=[
                    "canonical_name", "display_name", "category", "quadrant", "popularity",
                    "net_revenue", "total_qty", "orders_with_item", "avg_unit_price",
                    "avg_net_rev_per_unit", "avg_est_margin_per_unit", "est_margin_pct",
                    "discount_rate", "cost_coverage", "margin_source",
                    "recommended_action", "confidence",
                ]
            )

        result = kpis.copy()
        result["recommended_action"] = result.apply(
            lambda row: recommended_action(row["quadrant"], row["net_revenue"]),
            axis=1,
        )
        result["confidence"] = result.apply(
            lambda row: confidence_label(row["orders_with_item"], row["cost_coverage"]),
            axis=1,
        )
        return result[
            [
                "canonical_name", "display_name", "category", "quadrant", "popularity",
                "net_revenue", "total_qty", "orders_with_item", "avg_unit_price",
                "avg_net_rev_per_unit", "avg_est_margin_per_unit", "est_margin_pct",
                "discount_rate", "cost_coverage", "margin_source",
                "recommended_action", "confidence",
            ]
        ].sort_values("net_revenue", ascending=False)

    def get_bundle_opportunities(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        gold = self._get_obj5_gold(location_ids, start_date, end_date)
        empty_columns = [
            "item_a", "item_b", "display_a", "display_b", "category_a", "category_b",
            "pair_type", "pair_count", "support", "orders_a", "orders_b",
            "confidence_b_given_a", "confidence_a_given_b", "lift",
        ]
        if gold.empty:
            return pd.DataFrame(columns=empty_columns)

        order_items = gold[
            ["order_guid", "canonical_name", "display_name", "category_filled", "is_drink"]
        ].drop_duplicates()
        total_orders = order_items["order_guid"].nunique()
        item_support = (
            order_items.groupby("canonical_name")
            .agg(
                display_name=("display_name", "first"),
                category=("category_filled", "first"),
                is_drink=("is_drink", "first"),
                orders=("order_guid", "nunique"),
            )
        )

        rows = []
        for _, grp in order_items.groupby("order_guid"):
            items = grp.sort_values("canonical_name").to_dict("records")
            for i, item_a in enumerate(items):
                for item_b in items[i + 1:]:
                    rows.append((item_a["canonical_name"], item_b["canonical_name"]))
        if not rows:
            return pd.DataFrame(columns=empty_columns)

        pairs = (
            pd.DataFrame(rows, columns=["item_a", "item_b"])
            .value_counts(["item_a", "item_b"])
            .reset_index(name="pair_count")
        )
        pairs = pairs[pairs["pair_count"] >= 50]
        if pairs.empty:
            return pd.DataFrame(columns=empty_columns)

        def _build_pair(row):
            ia = item_support.loc[row["item_a"]]
            ib = item_support.loc[row["item_b"]]
            support = row["pair_count"] / total_orders if total_orders else 0
            denom = (ia["orders"] / total_orders) * (ib["orders"] / total_orders) if total_orders else 0
            lift = support / denom if denom else 0
            pair_type = (
                "DRINK+DRINK" if ia["is_drink"] and ib["is_drink"]
                else "FOOD+DRINK" if ia["is_drink"] or ib["is_drink"]
                else "FOOD+FOOD"
            )
            return pd.Series({
                "display_a": ia["display_name"],
                "display_b": ib["display_name"],
                "category_a": ia["category"],
                "category_b": ib["category"],
                "pair_type": pair_type,
                "support": support,
                "orders_a": ia["orders"],
                "orders_b": ib["orders"],
                "confidence_b_given_a": row["pair_count"] / ia["orders"] if ia["orders"] else 0,
                "confidence_a_given_b": row["pair_count"] / ib["orders"] if ib["orders"] else 0,
                "lift": lift,
            })

        enriched = pd.concat([pairs, pairs.apply(_build_pair, axis=1)], axis=1)
        enriched = enriched[enriched["lift"] >= 1.2]
        if enriched.empty:
            return pd.DataFrame(columns=empty_columns)
        return enriched.sort_values(["lift", "pair_count"], ascending=[False, False]).head(100)

    def get_promo_opportunities(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        gold = self._get_obj5_gold(location_ids, start_date, end_date)
        if gold.empty:
            return pd.DataFrame()

        daily = (
            gold.groupby(["canonical_name", "display_name", "category_filled", "location_id", "business_date"])
            .agg(
                daily_qty=("quantity", "sum"),
                avg_unit_price=("unit_price", "mean"),
                discount_exposed=("is_discounted", "max"),
            )
            .reset_index()
        )
        rows = []
        for item, grp in daily.groupby("canonical_name"):
            disc = grp[grp["discount_exposed"]]
            regular = grp[~grp["discount_exposed"]]
            if len(grp) < 10:
                continue
            avg_disc = disc["daily_qty"].mean() if not disc.empty else np.nan
            avg_regular = regular["daily_qty"].mean() if not regular.empty else np.nan
            price_range = grp["avg_unit_price"].max() - grp["avg_unit_price"].min()
            unique_prices = grp["avg_unit_price"].round(2).nunique()
            opportunity = "Monitor"
            if len(disc) >= 5 and pd.notna(avg_regular) and avg_disc > avg_regular:
                opportunity = "Promo candidate"
            elif unique_prices >= 3:
                opportunity = "Review price variation"
            rows.append({
                "canonical_name": item,
                "display_name": grp["display_name"].iloc[0],
                "category": grp["category_filled"].iloc[0],
                "store_days": len(grp),
                "discount_days": len(disc),
                "avg_qty_discounted_days": avg_disc,
                "avg_qty_regular_days": avg_regular,
                "qty_lift_on_discount_days": avg_disc - avg_regular if pd.notna(avg_disc) and pd.notna(avg_regular) else np.nan,
                "unique_prices": unique_prices,
                "min_price": grp["avg_unit_price"].min(),
                "max_price": grp["avg_unit_price"].max(),
                "price_range": price_range,
                "opportunity_type": opportunity,
            })

        if not rows:
            return pd.DataFrame()
        priority = {"Promo candidate": 0, "Review price variation": 1, "Monitor": 2}
        result = pd.DataFrame(rows)
        result["_priority"] = result["opportunity_type"].map(priority).fillna(3)
        return result.sort_values(
            ["_priority", "qty_lift_on_discount_days", "price_range"],
            ascending=[True, False, False],
        ).drop(columns="_priority")

    def get_price_margin_candidates(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        recommendations = self.get_menu_recommendations(location_ids, start_date, end_date)
        if recommendations.empty:
            return pd.DataFrame()
        return recommendations[
            recommendations["recommended_action"].isin(["Re-price", "Rework", "Remove"])
        ].sort_values(["recommended_action", "net_revenue"], ascending=[True, False])

    def get_day_of_week_index(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        df = self._filter_orders(location_ids, start_date, end_date)
        if df.empty:
            return pd.DataFrame(columns=["day", "index", "avg_revenue"])

        df = df.copy()
        df["_dt"] = pd.to_datetime(df["business_date"], format="%Y%m%d")
        df["_dow"] = df["_dt"].dt.dayofweek

        daily = (
            df.groupby(["business_date", "_dow"])["total_amount"]
            .sum()
            .reset_index()
            .rename(columns={"total_amount": "day_revenue"})
        )
        dow_avg = daily.groupby("_dow")["day_revenue"].mean().reset_index()
        overall_mean = dow_avg["day_revenue"].mean()
        dow_avg["index"] = dow_avg["day_revenue"] / overall_mean
        dow_avg["day"] = dow_avg["_dow"].map(
            {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
        )
        dow_avg = dow_avg.rename(columns={"day_revenue": "avg_revenue"})
        return dow_avg.sort_values("_dow")[["day", "index", "avg_revenue"]].reset_index(drop=True)

    def get_rfm_segments(
        self, location_ids: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        cust = self._customers[self._customers["location_id"].isin(location_ids)]
        orders = self._filter_orders(location_ids, start_date, end_date)
        joined = cust[["customer_id", "location_id", "order_guid"]].merge(
            orders[["order_guid", "total_amount", "business_date"]], on="order_guid"
        )
        if joined.empty:
            return pd.DataFrame(
                columns=["segment", "n_customers", "pct", "avg_total_spend", "avg_visits", "avg_recency_days"]
            )

        snapshot = joined["business_date"].max()
        agg = (
            joined.groupby("customer_id")
            .agg(
                total_spend=("total_amount", "sum"),
                visit_days=("business_date", "nunique"),
                last_visit=("business_date", "max"),
            )
            .reset_index()
        )
        agg["recency_days"] = agg["last_visit"].apply(
            lambda d: (
                pd.to_datetime(str(snapshot), format="%Y%m%d")
                - pd.to_datetime(str(d), format="%Y%m%d")
            ).days
        )

        m_75 = agg["total_spend"].quantile(0.75)
        r_recent = agg["recency_days"].quantile(0.25)
        r_stale = agg["recency_days"].quantile(0.75)
        m_low = agg["total_spend"].quantile(0.25)

        def _segment(row):
            f, m, r = row["visit_days"], row["total_spend"], row["recency_days"]
            if f >= 3 and r <= r_recent and m >= m_75:
                return "Champions"
            if f >= 2 and m >= m_75:
                return "Loyal high value"
            if f == 1 and r <= r_recent:
                return "Rising"
            if f == 1 and m >= m_75:
                return "Big-ticket occasional"
            if f >= 2 and r > r_stale:
                return "Cooling off"
            if f >= 2 and m < m_low:
                return "Budget regular"
            return "One-time"

        agg["segment"] = agg.apply(_segment, axis=1)
        total = len(agg)
        summary = (
            agg.groupby("segment")
            .agg(
                n_customers=("customer_id", "count"),
                avg_total_spend=("total_spend", "mean"),
                avg_visits=("visit_days", "mean"),
                avg_recency_days=("recency_days", "mean"),
            )
            .reset_index()
        )
        summary["pct"] = (summary["n_customers"] / total * 100).round(1)
        return summary.sort_values("avg_total_spend", ascending=False)

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
        elif marker == "specific_item_performance":
            item_name = param_map.get("item_name")
            items = self._filter_items(location_ids, start_date, end_date)
            if item_name:
                items = items[items["item_name"] == item_name]
            if items.empty:
                df = pd.DataFrame(
                    columns=["date", "item", "category", "order_count", "revenue", "avg_price"]
                )
            else:
                price_col = self._item_price_column(items)
                df = (
                    items.groupby(["business_date", "item_name"])
                    .agg(
                        category=("category", "first"),
                        order_count=("quantity", "sum"),
                        revenue=("total_price", "sum"),
                        avg_price=(price_col, "mean"),
                    )
                    .reset_index()
                    .rename(columns={"business_date": "date", "item_name": "item"})
                    .sort_values("date")
                )
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
