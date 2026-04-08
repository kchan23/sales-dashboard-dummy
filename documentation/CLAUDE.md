# CLAUDE.md

This file provides guidance to Claude Code when working with this repository.

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Restaurant Locations](#restaurant-locations)
- [Script Execution](#script-execution)
- [BigQuery Schema](#bigquery-schema)
- [Development Commands](#development-commands)
- [Known Data Quality Issues](#known-data-quality-issues)
- [Common Issues](#common-issues)

---

## Project Overview

**DoughZone Analytics Dashboard** — A Streamlit-based analytics dashboard for the DoughZone pizza restaurant chain. Ingests live POS data from the Toast API, stores it in Google BigQuery, and provides interactive dashboards with AI-powered natural language querying.

Two restaurant locations: South San Jose and Cupertino.

---

## Architecture

### Current Data Pipeline
```
Toast API
  └─ toast_api/scheduler.py       ← incremental daily pulls, writes raw rows
       └─ BigQuery raw tables     ← orders, order_items, payments, customer_orders, inventory
            └─ BigQuery views     ← *_clean views with type casts, dedup, derived columns
                 ├─ app.py        ← Streamlit dashboard (reads from views)
                 ├─ scripts/export_to_parquet.py  ← offline analysis exports
                 └─ analysis/exploratory_methods.ipynb
```

### Key Components

| File | Purpose |
|------|---------|
| `app.py` | Main Streamlit dashboard |
| `database/bigquery.py` | `BigQueryManager` — all BQ operations; analytics methods cached with `@st.cache_data(ttl=3600)` |
| `database/create_views.py` | Creates/replaces all BigQuery views |
| `toast_api/client.py` | Toast API OAuth client |
| `toast_api/scheduler.py` | Incremental data puller (runs daily or via backfill) |
| `toast_api/transformer.py` | Transforms raw API JSON → BigQuery row dicts |
| `scripts/test_pull.py` | Diagnostic test pull — prints to stdout, NO BigQuery writes |
| `scripts/export_to_parquet.py` | Exports BigQuery views to `exports/*.parquet` |
| `analysis/exploratory_methods.ipynb` | Jupyter notebook: time-series, OLS, K-Means, customer analysis |

---

## Restaurant Locations

| Name | GUID |
|------|------|
| Location A | `<your-restaurant-guid-1>` |
| Location B | `<your-restaurant-guid-2>` |

GUIDs are also stored in `toast_api/location_names.json` (auto-updated on each scheduler run).
Replace placeholder GUIDs with your actual restaurant GUIDs from the Toast API.

---

## Script Execution

**All scripts use relative imports and MUST be run as modules from the project root:**

```bash
cd /home/kchan23/cpp/capstone/sales-dashboard-app

# Diagnostic test pull (no BigQuery writes)
python3 -m scripts.test_pull --restaurant-id <GUID> --start-date 2026-03-01 --end-date 2026-03-01

# Full incremental scheduler pull (writes to BigQuery)
python3 -m toast_api.scheduler

# Backfill for a specific date range
python3 -m toast_api.scheduler --start-date 20241231 --end-date 20260308

# Backfill customer_orders ONLY (avoids duplicating other tables)
python3 -m toast_api.scheduler --start-date 20241231 --end-date 20260308 --customer-only

# Create/replace BigQuery views
python3 -m database.create_views

# Export parquet files
python3 -m scripts.export_to_parquet

# Start Streamlit dashboard
streamlit run app.py
```

Running `python3 scripts/test_pull.py` directly will fail with `ModuleNotFoundError: No module named 'toast_api'`.

---

## BigQuery Schema

### Raw Tables (append-only, no dedup)
| Table | Description |
|-------|-------------|
| `orders` | One row per order — totals, tips, discounts |
| `order_items` | One row per line item within each order |
| `payments` | One row per payment transaction |
| `customer_orders` | Customer PII per check (requires `guest.pi:read` scope) |
| `inventory` | Menu item snapshot from menus API (NOT real stock levels) |
| `import_log` | Records of all scheduler runs |

### Views (analytics-ready, deduped)
| View | Description |
|------|-------------|
| `orders_clean` | Deduped on `order_guid`, casts, derived columns (`tip_rate`, `hour_of_day`, `order_category`) |
| `order_items_clean` | Deduped (keep lowest `prediscount_total`), joined to `menu_canonical_map` for normalized names and category fallback; adds `true_unit_price = SAFE_DIVIDE(total_price, quantity)` |
| `payments_clean` | Type casts, NULL normalization |
| `daily_sales` | Aggregated by `location_id` + `business_date` |
| `item_performance` | Aggregated by item + location + date |
| `menu_canonical_map` | Item name normalization mapping; includes `category` column sourced from `inventory` |
| `customer_orders_masked` | PII replaced with SHA256 hash (`customer_id`); safe for analysis |

`customer_orders_clean` was intentionally removed — it exposed raw PII (email, phone, name) and was unused by the analytics pipeline. Use `customer_orders_masked` for all customer analysis.

**Never query raw tables directly for analysis — use `*_clean` views.**
Raw tables may contain duplicates from dual ingestion paths (CSV imports + Toast API). Deduplication is handled at the view layer via `ROW_NUMBER() OVER (PARTITION BY order_guid)`.

`get_sales_summary()` was fixed (March 2026) to query `orders_clean` instead of raw `orders` after revenue totals were found to be ~2× inflated due to duplicate order GUIDs. `orders_clean.business_date` is `DATE` type — use `PARSE_DATE('%Y%m%d', ...)` when filtering by date string params.

### Parquet Exports (`exports/`)
Exported from views (not raw tables). `customer_orders_masked.parquet` is the only customer export — PII is never exported in plaintext.

---

## Development Commands

```bash
# Check what's in BigQuery
python3 -c "
from database.bigquery import BigQueryManager
bq = BigQueryManager()
df = bq.client.query('SELECT COUNT(*) FROM \`{d}.orders\`'.format(d=bq.dataset_ref)).to_dataframe()
print(df)
"

# Recreate all views after schema changes
python3 -m database.create_views

# Re-export all parquet files
python3 -m scripts.export_to_parquet

# Run dashboard
streamlit run app.py  # http://localhost:8501
```

---

## Known Data Quality Issues

Discovered during exploratory analysis — discuss with client before building features that depend on these fields.

| Field | Issue |
|-------|-------|
| `hour_of_day` | Only 3.8% populated (Cupertino only, Jan–Feb 2025). Most order types don't include a timestamp. Hourly rush analysis not viable at scale. |
| `payment_date` | 100% NULL across all 179K payment rows. Payment timing analysis not possible. |
| `order_category` | 96.1% classified as "Other". Only DELIVERY and TAKEOUT have meaningful labels. |
| `inventory.current_stock` / `reorder_level` | Always 0. Toast menus API has no stock counts. The `inventory` table is a menu catalog, not a stock tracker. |
| South San Jose delivery | Near-zero delivery order volume. Location likely not enrolled in delivery platforms. |

---

## Common Issues

### `ModuleNotFoundError: No module named 'toast_api'`
Run scripts as modules from project root: `python3 -m scripts.test_pull ...`

### `customer_orders` is empty
Check if `guest.pi:read` OAuth scope is active. If scope is active but table is empty, the backfill may have run before `transform_customer_orders` was added. Re-run with `--customer-only` flag.

### Re-running scheduler creates duplicate rows
`stream_rows()` is a raw INSERT — there is no upsert logic. Only use `--start-date` override for date ranges that have NOT already been pulled, or use `--customer-only` to safely re-pull just customer data.

### "BigQuery project ID not found"
Ensure `GCS_PROJECT_ID` is set in `.env` or `GOOGLE_APPLICATION_CREDENTIALS` points to a valid service account JSON.

### Toast API 429 (rate limited)
The scheduler has built-in retry with `Retry-After` header support. Wait and re-run; do not loop manually.

### Revenue total appears inflated (double-counting)
Caused by querying the raw `orders` table instead of `orders_clean`. The raw table accumulates duplicate `order_guid` rows from both CSV imports and API pulls. Fixed in `database/bigquery.py:get_sales_summary()` — now queries `orders_clean`. Run this diagnostic to confirm dedup is working:
```sql
SELECT order_guid, COUNT(*) FROM `doughzone_analytics.orders` GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 10;
SELECT SUM(total_amount) FROM `doughzone_analytics.orders`;
SELECT SUM(total_amount) FROM `doughzone_analytics.orders_clean`;
```
