# Decision Log

Key technical and product decisions made during development, with rationale. Consult this before re-opening solved problems.

---

## Data & Privacy

### SHA256 pseudonymization for customer PII (not stars masking)
**Decision:** Hash customer email and phone with `TO_HEX(SHA256(...))` in the `customer_orders_masked` BigQuery view. Email takes priority; phone (digits-only normalized) is used as fallback.

**Why:** Stars masking (e.g., `j***@g***.com`) causes high collision rates — different customers with similar emails/phones become indistinguishable. SHA256 is deterministic (same input always produces same hash), collision-resistant (2²⁵⁶ hash space), and preserves join-ability across tables and DISTINCT customer counts. Name fields are dropped entirely.

**Impact:** Analytics can count unique customers, track repeat visits, and build cohorts without ever exposing raw PII.

---

### `customer_orders_clean` view removed
**Decision:** The `customer_orders_clean` BigQuery view was deleted from `database/create_views.py`. It is no longer created or referenced anywhere.

**Why:** The view exposed raw PII (first_name, last_name, customer_email, customer_phone) and was never used by the analytics pipeline. `customer_orders_masked` (SHA256-hashed) covers all analytical needs. Removing the view eliminates the risk of it being accidentally queried or referenced in future LLM-generated SQL.

**Impact:** Any direct references to `customer_orders_clean` will now fail at the BigQuery layer. The privacy check in `query/validator.py` also blocks any LLM-generated SQL that references this table.

---

### Small-n suppression in query results and customer analytics
**Decision:** `app.py` applies two suppression rules: (1) `_apply_small_n_suppression()` removes rows where any count-like column is below 5 from LLM query results; (2) the customer analytics tab requires at least 10 identified customers before rendering metrics (`CUSTOMER_MIN_N = 10`).

**Why:** Small groups can inadvertently expose individual-level behaviour even in aggregate outputs. Suppressing groups with fewer than 5 records is a standard de-identification threshold. The customer analytics minimum prevents misleading metrics (e.g., a "50% repeat rate" from 2 customers).

**Enforcement is layered:** The LLM prompt and SQL examples include `HAVING COUNT(*) >= 5` to suppress small groups at the database level; `_apply_small_n_suppression()` provides a second line of defence in Python; the customer tab threshold prevents rendering entirely when data is too sparse.

---

### Export `customer_orders_masked`, not `customer_orders_clean`
**Decision:** `scripts/export_to_parquet.py` exports `customer_orders_masked` (hashed). `customer_orders_clean` (raw PII) is intentionally excluded from parquet exports.

**Why:** Parquet files are used for offline analysis in Jupyter notebooks and may be stored locally or shared. PII must never appear in plaintext export files. The masked version is sufficient for all analytical use cases.

---

## Infrastructure & Pipeline

### Revenue double-counting bug (fixed March 2026)
**Decision:** `database/bigquery.py:get_sales_summary()` now queries `orders_clean` instead of the raw `orders` table.

**Why:** The raw `orders` table receives inserts from two sources — CSV/Excel file imports (`database/import_data.py`) and scheduled Toast API pulls (`toast_api/scheduler.py`). Because `stream_rows()` is a plain INSERT with no upsert logic, the same order GUID can appear multiple times in the raw table. Querying it directly for revenue totals caused gross double-counting (observed: ~$2M reported for 3 months across 2 locations). `orders_clean` deduplicates on `order_guid` via `ROW_NUMBER() OVER (PARTITION BY order_guid ORDER BY created_at)`, producing accurate totals.

**Schema note:** `orders_clean.business_date` is a `DATE` type (not STRING). The query uses `PARSE_DATE('%Y%m%d', @start_date)` for the WHERE clause and `FORMAT_DATE('%Y%m%d', business_date)` in the SELECT to keep the string format consistent with the rest of the app. `order_category` ('Delivery', 'Dine-In', 'Takeout') replaces raw `order_type` strings in CASE expressions.

---

### `stream_rows()` is raw INSERT — dedup is handled at the view layer
**Decision:** `database/bigquery.py:stream_rows()` uses BigQuery's `insert_rows_json()` — a plain append. No upsert/MERGE logic exists.

**Why:** BigQuery streaming inserts don't support MERGE natively in a simple way. Deduplication is instead handled in `database/create_views.py` using `ROW_NUMBER() OVER (PARTITION BY order_guid ORDER BY created_at)` in each `*_clean` view.

**Consequence:** Never re-run the scheduler for date ranges already pulled, unless using `--customer-only`. Duplicate raw rows are harmless (views deduplicate them) but waste storage.

---

### `--customer-only` flag added to scheduler
**Decision:** `toast_api/scheduler.py` has a `--customer-only` CLI flag that fetches orders from the API but only writes to the `customer_orders` table (skips orders, order_items, payments, menus).

**Why:** The original backfill ran before `transform_customer_orders` was wired into the scheduler. Re-running the full scheduler for the already-pulled date range would have created duplicate rows in all other tables. This flag allows safe single-table backfills.

**When to use:** Whenever `customer_orders` is empty or behind and other tables are already current.

---

### `customer_orders` requires `guest.pi:read` OAuth scope
**Decision:** The `customer_orders` table depends on Toast's `guest.pi:read` OAuth scope being active on the API credentials.

**Why:** The scope IS active on this integration. However, the initial backfill ran before `transform_customer_orders` was added to the scheduler, leaving the table empty. After re-running with `--customer-only`, the table was populated.

**To verify scope is active:** Make a live API call and inspect `checks[].customer` fields — they should contain email/phone/name values, not nulls.

---

## Product Scope

### `inventory` table is a menu catalog, not a stock tracker
**Decision:** The Inventory tab in the dashboard should be renamed/reframed as "Menu" or "Menu Catalog". Stock level gauges and reorder alerts are not viable from this data source.

**Why:** The Toast menus API (`/menus/v2/menus`) does not expose real-time stock counts. All rows in `inventory` have `current_stock = 0` and `reorder_level = 0` — these are hardcoded placeholders. Real inventory tracking would require Toast's inventory module (a separate feature the client may not have) or a third-party inventory system.

**What the table IS useful for:** Menu item names, categories, listed prices, and price change tracking across snapshots.

---

### Customer analytics depend on `guest.pi:read` scope and data coverage
**Decision:** Customer analytics features (unique customer counts, repeat rate, cohort retention) are only viable if `customer_orders` has meaningful data.

**Why:** Even with the scope active, coverage may be partial — only orders where the customer provided contact info (email or phone) at checkout produce a `customer_id` hash. Walk-in orders with no customer data produce `customer_id = NULL`.

**Consequence:** Repeat customer rate and cohort analyses undercount true repeat customers. Frame metrics as "of identifiable customers" rather than "of all customers."

---

## Architecture & Technology Choices

### React → Streamlit conversion
**Decision:** The dashboard was originally prototyped as a React component (`doughzone_dashboard.tsx`). It was rewritten in Streamlit.

**Why:** Python/Streamlit is easier to deploy, eliminates a separate frontend build step, and keeps the full stack in Python — the same language used for data processing and BigQuery queries. Streamlit Cloud was the target deployment platform, which natively supports Python apps.

---

### SQLite → BigQuery migration
**Decision:** The original data store was SQLite (`doughzone_analytics.db`). The project migrated to Google BigQuery.

**Why:** SQLite cannot handle multi-location scale, has no cloud access for Streamlit Cloud deployment, and lacks the columnar query performance needed for analytics. BigQuery provides managed scaling, partitioning/clustering, and integrates natively with GCS and the rest of GCP.

**Note:** Some older documentation still references SQLite. Treat those references as legacy.

---

### CSV/Excel import pipeline → Toast API live integration
**Decision:** The original data pipeline read from local `data/[LOCATION_ID]/[YYYYMMDD]/` directories (CSV/Excel exports from Toast). This was replaced by a live Toast API integration.

**Why:** CSV exports require manual export steps and have significant lag. The Toast API provides real-time data with a consistent schema that can be pulled incrementally on a schedule, eliminating manual file handling.

**What remains from the old pipeline:** `database/import_data.py` and `automation/gcs_import_worker.py` still exist for historical CSV data but are not part of the active daily pipeline.

---

### BigQuery table clustering on `location_id` + `business_date`
**Decision:** All tables with a `business_date` column are clustered on `(location_id, business_date)`.

**Why:** The vast majority of dashboard queries filter by location and date range. Clustering on these columns significantly reduces bytes scanned and query cost in BigQuery.

---

### BigQuery views as the analytics layer (raw tables are immutable)
**Decision:** `database/create_views.py` creates `*_clean` and `*_masked` views over raw tables. All Streamlit queries and parquet exports use these views, never the raw tables directly.

**Why:** Raw tables are append-only with no upsert logic. Views centralize type casting, NULL normalization, deduplication (`ROW_NUMBER() OVER PARTITION BY`), and derived column logic in one place. If the raw data or cleaning rules change, only the view needs updating — not every downstream query.

**Note:** `get_sales_summary()` was previously querying the raw `orders` table, causing double-counted revenue when the same order appeared from both CSV imports and Toast API pulls. It was corrected to query `orders_clean`. See "Revenue double-counting" in the Infrastructure section below.

---

### `stream_rows()` batch size of 500 rows per request
**Decision:** `database/bigquery.py:stream_rows()` splits inserts into batches of 500 rows.

**Why:** BigQuery's streaming insert API has payload size limits. Large single-payload inserts were failing for high-volume tables (order_items). Batching to 500 rows per request improved reliability.

---

### `@st.cache_data(ttl=3600)` on all BigQuery analytics methods
**Decision:** All five analytics methods in `database/bigquery.py` (`get_sales_summary`, `get_menu_performance`, `get_inventory_status`, `get_reviews`, `get_labor_analytics`) are decorated with `@st.cache_data(ttl=3600)`.

**Why:** Without caching, every sidebar interaction (date range change, location toggle) triggered five fresh BigQuery queries. Since these are instance methods, `self` is renamed to `_self` — the underscore prefix tells Streamlit to skip hashing the `BigQueryManager` instance, while still keying the cache on the actual query parameters (location IDs, dates). The 1-hour TTL balances cost savings against data freshness; the dashboard is fed by a daily scheduler pull so sub-hour freshness is not required.

**Impact:** Repeated queries for the same parameter combination within an hour return cached DataFrames at zero BigQuery cost.

---

### Dual-mode LLM query pipeline (LLM primary, rule-based fallback)
**Decision:** The "Ask Data a Question" feature uses OpenRouter/GPT as the primary query generator, with `query/llm_generator.py`'s keyword-to-SQL rules as a fallback.

**Why:** LLM-based SQL generation handles open-ended questions and handles ambiguity (via `AmbiguityDetector`). The rule-based fallback ensures the feature still works when the API key is missing or the network is unavailable, which matters during demos.

---

### SQL four-stage validation before execution
**Decision:** All generated SQL (LLM or rule-based) passes through `query/validator.py`: security check → privacy check → parameter check → BigQuery dry-run.

**Why:** LLM-generated SQL can contain DDL/DML statements (`DROP`, `DELETE`, etc.), raw PII table references, or missing parameters. The privacy check (Stage 2) blocks queries against `customer_orders`/`customer_orders_clean` and bare `order_guid`/`order_id` SELECT columns. The dry-run catches syntax errors and schema mismatches before they reach the live dataset.

---

### Removed rule-based `query_generator.py` (March 2026)
**Decision:** The legacy `query_generator.py` keyword-to-SQL module was deleted. The LLM-based generator (`query/llm_generator.py`) is now the only query backend, with no rule-based fallback.

**Why:** Maintaining two parallel query pipelines created confusion about which one handled which queries. The LLM-based approach handles all supported query types with better coverage.

---

### Multi-location `multiselect` instead of single `selectbox`
**Decision:** The sidebar location selector was changed from a single `st.selectbox` to `st.multiselect` with all locations selected by default.

**Why:** The client has two locations and frequently needs cross-location comparisons. The single selector forced toggling between locations. Multi-select with "all by default" means the default view always shows the complete picture, with the option to drill down to one location.

---

### `location_names.json` cache to avoid raw UUIDs in the UI
**Decision:** `toast_api/location_names.json` maps restaurant GUIDs to human-readable names. `app.py` reads this file on startup via `load_location_map()`.

**Why:** Toast restaurant GUIDs are UUIDs (e.g., `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`). Displaying them directly in the dashboard is confusing to end users. The cache is auto-updated by `pull_restaurant()` on each successful scheduler run.

---

### SHA256 file-hash deduplication for GCS import worker
**Decision:** The GCS import worker (`automation/gcs_import_worker.py`) tracks file SHA256 hashes in an `import_queue` table to avoid re-importing already-processed files.

**Why:** The worker polls GCS on a schedule and re-downloads any file it hasn't seen before. Without hash-based deduplication, network retries or reprocessing would insert duplicate rows.

---

## Analysis

### K-Means used random 80/20 split instead of chronological
**Decision:** The K-Means segmentation in `exploratory_methods.ipynb` uses a random 80/20 train/test split rather than chronological.

**Why:** `hour_of_day` is only populated for 3.8% of orders (Cupertino, Jan–Feb 2025). A chronological split would have placed nearly all temporal features in the train set, making the test set degenerate. Random split gave Train silhouette=0.3597, Test=0.3569, Gap=0.8% — stable.

---

### `menu_canonical_map` extended with `category` column
**Decision:** `database/generate_menu_map.py` now queries the `inventory` table for the most-common non-null category per item and stores it in `menu_canonical_map.category`. The `order_items_clean` and `item_performance` views fall back to this value via `COALESCE(NULLIF(oi.category, ''), m.category)` when the order item's own category field is empty.

**Why:** Many `order_items` rows have an empty `category` field (the Toast export omits it for some order types). The `inventory` table (sourced from the Toast menus API) has more complete category coverage. Joining through `menu_canonical_map` fills the gap without duplicating the inventory-category logic in every view.

---

### LASSO dropped `discount_rate`, `tip_rate`, `delivery_mix` as revenue predictors
**Decision:** The LASSO-CV model for revenue prediction does not include these features.

**Why:** `delivery_mix` is near-constant for South San Jose (location not on delivery platforms). `discount_rate` and `tip_rate` have low predictive signal after regularization. The retained features (order count, subtotal components) are sufficient for Test R²=0.856.
