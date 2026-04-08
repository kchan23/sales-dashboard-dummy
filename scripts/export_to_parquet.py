#!/usr/bin/env python3
"""Export BigQuery tables to local parquet files for offline analysis."""

import logging
from pathlib import Path
from database.bigquery import BigQueryManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TABLES = [
    "orders_clean",
    "order_items_clean",
    "payments_clean",
    "daily_sales",
    "item_performance",
    "menu_canonical_map",
    "inventory",
    "customer_orders_masked",
]
OUTPUT_DIR = Path("exports")


def main():
    bq = BigQueryManager()
    OUTPUT_DIR.mkdir(exist_ok=True)

    for table in TABLES:
        logger.info(f"Exporting {table}...")
        df = bq.client.query(
            f"SELECT * FROM `{bq.dataset_ref}.{table}`"
        ).to_dataframe()
        out = OUTPUT_DIR / f"{table}.parquet"
        df.to_parquet(out, index=False)
        size_mb = out.stat().st_size / 1_000_000
        logger.info(f"  {table}: {len(df):,} rows -> {out} ({size_mb:.1f} MB)")

    logger.info(f"Done. Files in ./{OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
