# Changelog

## Table of Contents
- [2026-03-26 — Fix `unit_price` column bug & rename to `prediscount_total`](#2026-03-26---fix-unit_price-column-bug--rename-to-prediscount_total)
- [2026-03-23 — Privacy Guardrails & Category Fallback](#2026-03-23---privacy-guardrails--category-fallback)
- [2026-03-14 — Multi-Location Selector & Exploratory Analysis](#2026-03-14---multi-location-selector--exploratory-analysis)
- [2026-03-09 — Project Reorganization & Feature Updates](#2026-03-09---project-reorganization--feature-updates)
- [2025-12-10 — BigQuery Integration Fix](#2025-12-10---bigquery-integration-fix)
- [2025-12-10 — Week 2: Automated Import Pipeline](#2025-12-10---week-2-automated-import-pipeline)
- [2025-12-10 — Week 1: GCS Storage Integration](#2025-12-10---week-1-gcs-storage-integration-cloud-first-automation)
- [2025-11-24 — Weekly & Monthly Trend Analysis Support](#2025-11-24---weekly--monthly-trend-analysis-support)
- [2025-11-24 — Inventory Date Formatting & Last Ordered Population](#2025-11-24---inventory-date-formatting--last-ordered-population)
- [2025-11-24 — UI Navigation Redesign & Branding Update](#2025-11-24---ui-navigation-redesign--branding-update)
- [2025-11-24 — Natural Language Query & Date Formatting Feature](#2025-11-24---natural-language-query--date-formatting-feature)

## [2026-03-26] - Fix `unit_price` column bug & rename to `prediscount_total`

### Fixed
- `get_menu_performance()` in `database/bigquery.py` was referencing `oi.unit_price` which does not exist on the raw `order_items` table, causing a BigQuery `invalidQuery` error on the main dashboard. Replaced with `SAFE_DIVIDE(SUM(oi.total_price), SUM(oi.quantity))`.

### Changed — field rename: `unit_price` → `prediscount_total`
- The raw `order_items` column was previously named `unit_price` but was actually populated from Toast's `preDiscountPrice` (a line total, not a per-unit price). Renamed to `prediscount_total` to match its true semantics.
- Updated everywhere: `database/bigquery.py` (schema + query), `database/create_views.py` (dedup ORDER BY + view SELECT), `database/import_data.py`, `integrations/toast_api/transformer.py`, `integrations/toast_api/field_mapping.py`, `config/prompts.py` (schema description + 3 example queries), `documentation/data_dictionary.md`.
- `avg_unit_price` in the `item_performance` view now uses `SAFE_DIVIDE(SUM(total_price), SUM(quantity))` instead of `AVG(unit_price)`.
- `order_items_clean` view adds `true_unit_price = SAFE_DIVIDE(total_price, quantity)` as the correct per-unit price derivation.

---

## [2026-03-23] - Privacy Guardrails & Category Fallback

### Added — `app.py`
- `_apply_small_n_suppression(df, threshold=5)`: filters rows where any count-like column is below 5; applied to all LLM/rule-based query results with a visible warning when rows are suppressed
- `CUSTOMER_MIN_N = 10` threshold on the customer analytics tab — shows an "insufficient data" message instead of rendering misleading metrics when fewer than 10 identified customers are in the selected period

### Changed — `config/prompts.py`
- Added four new system prompt rules: aggregation-only results required, `HAVING COUNT(*) >= 5` on all GROUP BY queries, no raw order identifiers (`order_guid`, `order_id`) in SELECT output, no references to PII tables (`customer_orders`, `customer_orders_clean`)
- Added `HAVING COUNT(*) >= 5` to all 13 example queries
- Fixed misplaced `ORDER BY` in Example 18

### Changed — `query/validator.py`
- Added Stage 2 `_check_privacy_safety()` between the security check and parameter check
  - Blocks `customer_orders` and `customer_orders_clean` table references
  - Blocks bare `order_guid`/`order_id` as SELECT output columns (aggregate use permitted)
- Updated docstrings to reflect four-stage pipeline

### Changed — `database/create_views.py`
- `order_items_clean` and `item_performance`: `category` column now uses `COALESCE(NULLIF(oi.category, ''), m.category)` to fall back to `menu_canonical_map.category` when the order item's own category is empty
- **Removed `customer_orders_clean` view** — exposed raw PII (email, phone, first/last name) and was unused; replaced with a comment explaining the removal

### Changed — `database/generate_menu_map.py`
- Extended `menu_canonical_map` with a `category` column: queries `inventory` for the most-common non-null category per item using a `QUALIFY ROW_NUMBER()` window, then merges into the mapping table
- BigQuery schema updated to include `SchemaField("category", "STRING")`

### Changed — `.gitignore`
- Added `analysis/professor_viability_check.ipynb`

---

## [2026-03-14] - Multi-Location Selector & Exploratory Analysis

### Added — `integrations/toast_api/location_names.json`
- New persistent cache file mapping restaurant GUIDs to human-readable display names
- Populated with both known locations: Location A (`location-guid-placeholder-1`) and Location B (`location-guid-placeholder-2`)
- Written/updated automatically on each successful `pull_restaurant()` call
- Used by `app.py` on startup to avoid raw UUID exposure in the UI

### Added — `analysis/exploratory_methods.ipynb`
- New Jupyter notebook covering Part 1 (Time-Series), Part 2 (Driver Analysis), Part 3 (K-Means Segmentation)
- Implements chronological train/val/test split: Train ≤ 2025-10-31, Val ≤ 2025-12-31, Test > 2025-12-31
- ETS (Holt-Winters, seasonal_periods=7): Val MAE=$2,595 / MAPE=16.8%, Test MAE=$4,820 / MAPE=52.3%
- OLS with HC3 robust SEs: Train R²=0.954 (standardized betas; scaler fit on train only)
- LASSO-CV with `TimeSeriesSplit(n_splits=5, test_size=30)`: Test R²=0.856; dropped `discount_rate`, `tip_rate`, `delivery_mix`
- K-Means (k=4) stability check: random 80/20 split (temporal split not viable — see data quality notes); Train silhouette=0.3597, Test=0.3569, Gap=0.8% → STABLE

### Changed — `integrations/toast_api/scheduler.py`
- Added `json` and `pathlib.Path` imports
- Added `_update_location_name_cache(guid, name)` helper that writes/updates `integrations/toast_api/location_names.json` after each successful pull
- `pull_restaurant()` now calls `_update_location_name_cache()` before returning, ensuring the cache stays current

### Changed — `database/bigquery.py`
- All analytics query functions updated from `location_id: str` → `location_ids: List[str]`
- SQL filters changed from `WHERE location_id = @location_id` → `WHERE location_id IN UNNEST(@location_ids)`
- BigQuery parameters changed from `ScalarQueryParameter` → `ArrayQueryParameter("location_ids", "STRING", [...])`
- Affected functions: `get_sales_summary()`, `get_menu_performance()`, `get_inventory_status()`, `get_reviews()`, `get_labor_analytics()`, `get_available_dates()`

### Changed — `app.py`
- Added `import json` and `from pathlib import Path` to imports
- Added `load_location_map(db, toast_client=None)` helper — loads GUID→name from `integrations/toast_api/location_names.json` cache, falls back to live Toast API, falls back to anonymized "Location N" labels; **never exposes raw UUIDs in the UI**
- Replaced `st.sidebar.selectbox("Select Location", locations)` with `st.sidebar.multiselect("Select Location(s)", ...)` — all locations selected by default, built-in type-to-search filter, checkbox-style multi-selection
- All five DB data-load calls now pass `selected_locations` (list of UUIDs) instead of a single string
- LLM `generate_query()` calls pass `selected_locations[0]` as the location context (existing `LLMQueryGenerator` signature unchanged)
- Added guard: if no locations selected, shows warning and calls `st.stop()`

---

### Data Quality Findings (Client-Facing)

The following issues were discovered during exploratory analysis. These should be discussed with the client.

#### `hour_of_day` — Severely Limited Coverage
- Populated for **only 4,852 orders** out of 126K+ total (≈3.8%)
- All valid records fall within **Jan 9 – Feb 9, 2025** and are exclusively from **Cupertino**
- Root cause: `order_time` field is `NULL` for the vast majority of orders (order types `Order-and-Pay-at-Table`, `In Store`, `API` do not include an order timestamp in the Toast export)
- **Impact**: Hourly segmentation analysis (e.g., lunch/dinner rush identification) is not viable at scale; the K-Means temporal split had to use random 80/20 instead of chronological due to this gap
- **Recommendation**: Ask Toast to enable `order_time` export for all order types, or pull timestamps from the Toast API's raw order objects

#### Delivery Channel Discrepancy Between Locations
- **South San Jose** shows **near-zero delivery order volume** compared to **Cupertino**
- This is consistent with one location not being enrolled in a third-party delivery platform (e.g., DoorDash, Uber Eats)
- `delivery_mix` was dropped by LASSO as a predictor for the full dataset model — likely because the feature is near-constant for South San Jose
- **Impact**: Cross-location delivery analysis is not meaningful; any delivery-specific metrics should be segmented by location
- **Recommendation**: Confirm with client whether South San Jose is intentionally not on delivery platforms, and whether enrollment is planned

#### `payment_date` — 100% NULL
- The `payments_clean` table has `payment_date` null for all 179K rows
- Prevents payment timing analysis (e.g., split-check reconciliation, same-day vs. next-day settlement)
- **Recommendation**: Verify whether this field is exported by Toast; may require a different report type

#### `inventory.last_ordered` — 100% NULL (BigQuery)
- The `last_ordered` field in the `inventory` table has never been populated from Toast exports
- Prevents "days since last ordered" inventory aging analysis
- **Recommendation**: Check whether Toast's menu/inventory export includes this field, or populate via a join to `order_items`

#### `order_category` — 96.1% classified as "Other"
- All `Order-and-Pay-at-Table`, `In Store`, and `API` order types map to the catch-all "Other" category
- Only `DELIVERY` and `TAKEOUT` orders have meaningful category labels
- Makes `order_cat_enc` a near-useless feature in segmentation models
- **Recommendation**: If granular order type distinctions are needed, use `order_type` directly rather than the derived category field

---

## [2026-03-09] - Project Reorganization & Feature Updates

### Changed - Project Structure
- Reorganized root-level scripts into logical folders to reduce clutter
  - `llm_query_generator.py` → `query/llm_generator.py`
  - `sql_validator.py` → `query/validator.py`
  - `import_data.py` → `database/import_data.py`
  - `setup_check.py`, `test_gcs_setup.py`, `upload_to_gcs.py`, `export_to_parquet.py` → `scripts/`
  - `eda_cleaning.ipynb` → `notebooks/`
- New folders: `query/`, `scripts/`, `notebooks/`
- Updated all import paths affected by moves (`app.py`, `automation/gcs_import_worker.py`, `scripts/setup_check.py`)

### Removed
- **`query_generator.py`**: Deleted legacy rule-based SQL generator; fully superseded by `query/llm_generator.py`

### Changed - `app.py`
- Replaced dropdown date selectors with `st.date_input()` calendar widget for a cleaner date range UX
- Added **Toast API Sync** expander in sidebar — pulls latest data directly from Toast into BigQuery (2-minute cooldown, shows live spinner)
- Added `ToastAPIClient` and `pull_restaurant` imports

### Changed - `database/bigquery.py`
- `stream_rows()` now inserts in batches (default 500 rows per request) instead of one large payload, improving reliability for large imports

### Changed - `integrations/toast_api/scheduler.py`
- `compute_date_range()` and `pull_restaurant()` now accept `start_date_override` and `end_date_override` parameters for manual backfill control
- CLI gains `--start-date` and `--end-date` flags to override the automatic date range detection

### Added - New Files
- `database/__init__.py`: Package init for database module
- `database/create_views.py`: Creates cleaned analytics views (`orders_clean`, `order_items_clean`, `payments_clean`) with type casts, NULL normalization, and derived columns
- `database/generate_menu_map.py`: Menu mapping utilities
- `scripts/export_to_parquet.py`: Exports BigQuery tables to local parquet files for offline analysis
- `notebooks/eda_cleaning.ipynb`: Exploratory data analysis notebook

---

## [2025-12-10] - BigQuery Integration Fix

### Fixed
- **database/bigquery.py**: Fixed BigQueryManager initialization bug that prevented running outside Streamlit
  - Wrapped Streamlit secrets check in try/except to handle non-Streamlit execution contexts
  - Added `load_dotenv()` to properly load environment variables from .env file
  - Previously failed with `StreamlitSecretNotFoundError` when running import scripts

- **app.py**: Fixed "Ask Data a Question" feature for BigQuery compatibility
  - Changed from SQLite cursor methods (`fetchall()`, `description`) to BigQuery QueryJob methods
  - Now uses `query_job.to_dataframe()` to convert results directly to pandas DataFrame
  - Fixed AttributeError: 'QueryJob' object has no attribute 'fetchall'

- **app.py**: Fixed incorrect sorting in Menu Performance and Inventory pages
  - Removed premature string formatting of numeric columns (revenue, avg_price, unit_cost)
  - Now uses `st.column_config.NumberColumn()` for proper numeric formatting and sorting
  - Menu items now sort by revenue (descending) by default instead of order_count
  - Inventory items now sort by stock level (ascending) to show low-stock items first
  - Users can now click column headers to sort numerically instead of alphabetically

### Added
- **BigQuery Dataset**: Created `doughzone_analytics` dataset with 7 tables
  - orders (4,905 rows)
  - order_items (24,376 rows)
  - payments (0 rows)
  - inventory (2,782 rows)
  - reviews (0 rows)
  - time_entries (173 rows)
  - import_log (128 rows)

### Changed
- **import_data.py**: Updated to use CSV reader for AllItemsReport instead of Excel
  - Changed from `pd.read_excel()` to `pd.read_csv()` for better compatibility
  - Removed unnecessary `.xls` file processing

### Impact
- ✅ **Data pipeline now complete**: GCS → BigQuery → Streamlit Dashboard
- ✅ **32,236 rows imported** from 416 files across 32 dates
- ✅ **BigQuery tables now populated** and queryable from Streamlit app
- ✅ **Fixed root cause**: Dataset didn't exist because import scripts couldn't initialize BigQueryManager

### Root Cause Analysis
The issue was that while data files (416 files) were successfully uploaded to GCS, they were never being imported into BigQuery because:
1. The BigQuery dataset `doughzone_analytics` was never created
2. BigQueryManager failed to initialize when run outside Streamlit due to improper exception handling
3. Import scripts couldn't run successfully to create schema or import data

---

## [2025-12-10] - Week 2: Automated Import Pipeline

### Added - Database Schema
- **database.py**: `import_queue` table for tracking file processing
  - Tracks file hash (SHA256) for deduplication
  - Status tracking: pending/processing/completed/failed
  - Error message logging for failed imports
  - Rows imported counter per file

- **database.py**: Import queue management methods (8 new methods)
  - `add_to_import_queue()` - Add file to processing queue
  - `get_pending_imports()` - Retrieve unprocessed files
  - `mark_import_processing()` - Update status to processing
  - `mark_import_completed()` - Mark success with row count
  - `mark_import_failed()` - Log errors
  - `is_file_processed()` - Check if hash already imported
  - `get_import_queue_status()` - View queue history (audit trail)

### Added - Automation
- **automation/gcs_import_worker.py**: Automated import worker (~250 lines)
  - Polls GCS bucket every 5 minutes for new files
  - Downloads only new files (hash-based detection)
  - Runs incremental import automatically
  - Uploads updated database to GCS with versioned backups
  - Cleans up temporary files after processing

### Changed
- **import_data.py**: Added incremental import logic
  - `_calculate_file_hash()` - SHA256 hash calculation
  - `_should_process_file()` - Deduplication check before import
  - Modified `_process_date_directory()` - Hash checking integration
  - Enhanced import statistics (added 'skipped' and 'new_files' counters)
  - Improved summary output with skip counts
  - Added return values to `_process_csv_file()` and `_process_excel_file()`

- **requirements.txt**: Added worker dependency
  - schedule>=1.2.0 (for periodic task scheduling)

### Impact & Benefits
- ✅ Incremental imports - Only process new/changed files
- ✅ No duplicate data - Hash-based deduplication prevents re-imports
- ✅ Automated pipeline - Worker runs continuously without manual intervention
- ✅ Complete audit trail - import_queue table tracks all file processing
- ✅ Error recovery - Failed imports logged with error messages for debugging
- ✅ Database versioning - Timestamped backups created automatically
- ⏱️ Performance - Skips 100% of files on re-run (vs. re-processing all)

### Usage Examples
```bash
# Run import manually (incremental)
python3 import_data.py

# Start automated worker (runs every 5 minutes)
python3 -m automation.gcs_import_worker

# Check import queue status
python3 -c "from database import DatabaseManager; db = DatabaseManager(); db.connect(); print(db.get_import_queue_status())"
```

### Next Steps
- Week 3: Streamlit Cloud integration (auto-download DB from GCS, admin monitoring tab)

---

## [2025-12-10] - Week 1: GCS Storage Integration (Cloud-First Automation)

### Added - Core Infrastructure
- **automation/storage_sync.py**: GCS integration module (~350 lines)
  - Upload/download files to/from Google Cloud Storage with progress tracking
  - Database versioning with timestamped backups
  - File hash calculation (SHA256) for deduplication
  - Error handling with retries and graceful failures
  - Support for batch directory uploads
  - Metadata retrieval and file existence checks

- **upload_to_gcs.py**: Command-line upload helper script (~250 lines)
  - Interactive upload tool with file validation
  - Supports single date or all dates upload modes
  - Progress indicators and user confirmations
  - Custom bucket and credentials support
  - Validates file structure before upload (checks for CSV/Excel files)

- **test_gcs_setup.py**: GCS setup verification script
  - Checks dependencies installation
  - Validates credentials configuration
  - Tests GCS connection and permissions
  - Verifies upload capability

- **automation/__init__.py**: Package initialization
  - Exports GCSStorageSync and credential loading functions

### Added - Configuration & Documentation
- **.env.example**: Template for local GCS credentials
  - GCS_PROJECT_ID, GCS_BUCKET_NAME, GOOGLE_APPLICATION_CREDENTIALS

- **GCS_SETUP.md**: Comprehensive setup guide (2,000+ words)
  - Step-by-step GCP account and bucket creation
  - Service account setup with screenshots
  - Billing alerts configuration
  - Cost estimates and troubleshooting
  - Security best practices

### Changed
- **requirements.txt**: Added cloud storage dependencies
  - google-cloud-storage>=2.10.0
  - python-dotenv>=1.0.0
  - tqdm>=4.65.0 (progress bars)

- **.gitignore**: Enhanced security for GCS credentials
  - Added patterns: *-gcs-key.json, *service-account*.json, credentials.json
  - Protected .streamlit/secrets.toml from commits

### Impact & Benefits
- ✅ Cloud storage foundation for 27+ location scalability
- ✅ Automated upload workflow (no more manual import_data.py)
- ✅ Database versioning with timestamped backups
- ✅ Production-ready error handling and logging
- ✅ Ready for Streamlit Cloud integration (Week 3)
- 💰 Cost: ~$0.52/month (1 location), ~$7-14/month (27 locations)

### Usage Examples
```bash
# Upload all dates for a location
python upload_to_gcs.py --location 90984 --all-dates

# Upload specific date
python upload_to_gcs.py --location 90984 --date 20250210

# Test GCS setup
python test_gcs_setup.py
```

### Next Steps
- Week 2: Automated import pipeline (import_queue table, incremental imports, gcs_import_worker.py)
- Week 3: Streamlit Cloud integration (auto-download DB, admin monitoring tab)

---

## [2025-11-24] - Weekly & Monthly Trend Analysis Support

### Added
- **query_generator.py**: Support for weekly and monthly time period aggregations
  - New `_weekly_metric_trend_query()` for specific metrics by week (tax, tips, discounts)
  - New `_monthly_metric_trend_query()` for specific metrics by month
  - New `_weekly_trend_query()` for general revenue trends by week
  - New `_monthly_trend_query()` for general revenue trends by month
  - Automatic detection of "weekly", "by week", "w/w", "monthly", "by month" keywords

### Changed
- **query_generator.py**: Expanded trend detection to include weekly/monthly keywords alongside daily/trend/graph
  - Now prioritizes time-period keywords to route to appropriate grouping function
  - Uses SQLite string functions to convert YYYYMMDD format to proper date format

### Impact & Examples
- Natural language queries now support multiple time periods for flexible analysis
- Users can ask questions with different granularities:
  - Daily: "Show me daily revenue" → Daily breakdown (32 rows)
  - Weekly: "What's the weekly revenue like?" → Weekly breakdown (5 rows)
  - Monthly: "Show me monthly tax trends" → Monthly breakdown (2 rows)
- All metric types work with all time periods (revenue, tax, tips, discounts)
- Query results include appropriate aggregations (count, sum, average)

## [2025-11-24] - Inventory Date Formatting & Last Ordered Population

### Added
- **import_data.py**: Logic to populate `last_ordered` field by querying order history
  - For each inventory item, queries the most recent order date from order_items table
  - Successfully populated 85% of inventory items (items not ordered have NULL value)
  - Items with no order history (e.g., discontinued items, never-ordered menu items) remain NULL

### Changed
- **database.py**: Format `last_ordered` as MM/DD/YYYY in inventory queries
- **app.py**: Improved inventory column header from 'last_ordered' to 'Last Ordered'

### Fixed
- **inventory table**: `last_ordered` field now contains accurate order dates instead of always being NULL
  - Previous: All 2,779 inventory records had last_ordered = None
  - Current: 2,361 items (85%) show actual last order dates in MM/DD/YYYY format, 418 items (15%) with NULL indicate never-ordered items

## [2025-11-24] - UI Navigation Redesign & Branding Update

### Added
- **app.py**: Sidebar navigation with radio buttons for page selection instead of top tabs
- **app.py**: "Ask Data a Question" as dedicated front page landing experience

### Changed
- **app.py**: Replaced pizza icon (🍕) with Xiao Long Bao icon (🥟) to reflect Chinese cuisine branding
- **app.py**: Restructured navigation from horizontal tabs to vertical sidebar menu for better UX
- **app.py**: Page structure converted from `st.tabs()` to conditional page routing with if/elif statements

## [2025-11-24] - Natural Language Query & Date Formatting Feature

### Added
- **query_generator.py**: New module for converting natural language questions to SQL queries with support for:
  - Top items analysis (revenue and order count)
  - Revenue and sales metrics
  - Order type comparisons
  - Trend analysis over time
  - Inventory status queries
  - Order breakdown by type
  - Average metrics calculations
- **app.py**: "Ask Data a Question" feature with text input for natural language queries
- **app.py**: Automatic visualization generation for query results (line charts for trends, bar charts for categories)
- **app.py**: Date formatting to MM/DD/YYYY display format while maintaining YYYYMMDD database format
- **app.py**: Expandable SQL query viewer to show generated queries to users

### Fixed
- **requirements.txt**: Corrected openpyxl version from non-existent 3.11.0 to available 3.1.5
- **database.py**: Added `check_same_thread=False` parameter to sqlite3.connect() to resolve Streamlit threading errors
- **app.py**: Updated get_db() function to pass `check_same_thread=False` when connecting to database
- **import_data.py**: Fixed OrderDetails CSV column mappings (Order Id, Total, Tip, Discount Amount, Amount)
- **import_data.py**: Fixed ItemSelectionDetails CSV column mappings (Menu Item, Sales Category, Qty, Net Price)
- **import_data.py**: Changed logger.debug() to logger.warning() for better error visibility in imports
- **app.py**: Replaced non-existent px.barh() with px.bar(orientation='h') for horizontal bar charts

### Impact
- Successfully imported 9,825 orders with 24,376 order items from 3 locations across 32 dates
- Users can now ask questions like "What are the top 10 items by revenue?" and get instant visualizations
- Date selectors now display user-friendly MM/DD/YYYY format instead of database format YYYYMMDD
- Streamlit app no longer crashes with SQLite thread safety errors
- All menu performance charts now render correctly
