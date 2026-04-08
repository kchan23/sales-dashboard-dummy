# Data Dictionary — DoughZone Analytics

**Dataset:** `doughzone_analytics` (BigQuery)
**Data source:** Toast POS API (REST v2), CSV/Excel file imports via GCS
**Pipeline:** Toast API → BigQuery raw tables → derived views → Parquet exports

---

## Table of Contents
- [Global Conventions](#global-conventions)
- [Derived Views](#derived-views)
  - [`orders_clean`](#orders_clean)
  - [`order_items_clean`](#order_items_clean)
  - [`payments_clean`](#payments_clean)
  - [`daily_sales`](#daily_sales)
  - [`item_performance`](#item_performance)
- [Toast API Field Lineage](#toast-api-field-lineage)
  - [Orders](#orders--ordersbulkv2)
  - [Order Items](#order-items--orderschecksselections)
  - [Payments](#payments--orderschecks-payments)
  - [Menu / Inventory](#menu--inventory--menusv2menus)
  - [Guest / Customer Data](#guest--customer-data)
  - [Labor — Future](#labor--future-not-yet-active)
- [Parquet Exports](#parquet-exports)

---

## Global Conventions

| Convention | Detail |
|---|---|
| Date format | `STRING YYYYMMDD` in raw tables (e.g. `"20260307"`). Cast to `DATE` via `PARSE_DATE('%Y%m%d', business_date)` in views. |
| Timestamp format | ISO 8601 string (e.g. `"2026-03-07T00:07:04.648+0000"`). Cast via `SAFE.PARSE_TIMESTAMP(...)` in views. Fallback: `'%m/%d/%y %I:%M %p'` for CSV-sourced data. |
| Money fields | `FLOAT64`, rounded to 2 decimal places. |
| Sentinel values | `'UNKNOWN'` and `''` are replaced with `NULL` in all clean views. |
| Location IDs | `STRING` (e.g. `"90984"`). |
| Deduplication | `orders_clean` deduplicates on `order_guid` (keep earliest `created_at`). `order_items_clean` deduplicates on `(order_guid, item_name, quantity)` (keep lowest `prediscount_total`). |

---

## Derived Views

These are the primary analytics-ready surfaces used by the dashboard. All raw tables are accessed only through these views.

---

### `orders_clean`

**Source:** `orders` raw table
**Defined in:** [`database/create_views.py`](../database/create_views.py)
**Purpose:** Cleaned, deduplicated orders with parsed timestamps, normalized order categories, and outlier flags.

| Column | Type | Description | Example |
|---|---|---|---|
| `order_id` | STRING | Unique order identifier (same value as `order_guid`) | `"abc-123"` |
| `location_id` | STRING | Restaurant location ID | `"90984"` |
| `business_date` | DATE | Business date, parsed from raw YYYYMMDD string | `2026-03-07` |
| `order_guid` | STRING | Toast GUID for the order | `"abc-123"` |
| `order_time` | TIMESTAMP | Time the order was opened; parsed from ISO 8601 with CSV fallback | `2026-03-07 00:07:04 UTC` |
| `order_type` | STRING | Raw dining option from Toast (`NULL` if was `'UNKNOWN'`) | `"DELIVERY"` |
| `order_category` | STRING | Normalized category derived from `order_type`. One of: `Delivery`, `Takeout`, `Dine-In`, `Other` | `"Delivery"` |
| `total_amount` | FLOAT | Order total including tax and tips | `45.50` |
| `subtotal` | FLOAT | Pre-discount subtotal | `40.00` |
| `tax_amount` | FLOAT | Tax collected | `3.50` |
| `tip_amount` | FLOAT | Tips received | `8.00` |
| `discount_amount` | FLOAT | Discounts applied | `2.00` |
| `tip_rate` | FLOAT | `tip_amount / total_amount` (NULL-safe divide) | `0.176` |
| `hour_of_day` | INTEGER | Hour extracted from `order_time` (0–23) | `0` |
| `day_of_week` | STRING | Full weekday name derived from `business_date` | `"Friday"` |
| `flag_high_total` | BOOLEAN | `TRUE` if `total_amount > 500` | `FALSE` |
| `flag_negative` | BOOLEAN | `TRUE` if `total_amount < 0` (refunds/voids) | `FALSE` |
| `flag_zero_amount` | BOOLEAN | `TRUE` if `total_amount = 0` | `FALSE` |
| `created_at` | TIMESTAMP | Row insertion timestamp (set by BigQuery on insert) | `2026-03-07 01:00:00 UTC` |

**`order_category` mapping logic:**

| Raw `order_type` contains | → `order_category` |
|---|---|
| `DELIVERY` | `Delivery` |
| `TAKEOUT`, `TAKE OUT`, `PICKUP`, `PICK UP` | `Takeout` |
| `DINE` | `Dine-In` |
| Anything else | `Other` |

---

### `order_items_clean`

**Source:** `order_items` raw table LEFT JOIN `menu_canonical_map`
**Defined in:** [`database/create_views.py`](../database/create_views.py)
**Purpose:** Deduplicated line items with canonical item names resolved via the menu mapping table.

| Column | Type | Description | Example |
|---|---|---|---|
| `order_guid` | STRING | FK — links to `orders_clean.order_guid` | `"abc-123"` |
| `item_name` | STRING | Raw item name from Toast (`NULL` if blank) | `"Pepperoni Pizza"` |
| `canonical_name` | STRING | Standard item key from `menu_canonical_map`; falls back to `item_name` if no match | `"pepperoni_pizza"` |
| `display_name` | STRING | Human-readable name from `menu_canonical_map`; falls back to `item_name` if no match | `"Pepperoni Pizza"` |
| `category` | STRING | Menu sales category (`NULL` if blank) | `"Pizza"` |
| `quantity` | INTEGER | Number of this item ordered | `2` |
| `prediscount_total` | FLOAT | Pre-discount line total from Toast (`preDiscountPrice`); quantity × unit price before discounts | `25.98` |
| `total_price` | FLOAT | Actual line total after discounts (`price` in Toast API) | `25.98` |
| `location_id` | STRING | Denormalized restaurant location ID | `"90984"` |
| `business_date` | DATE | Business date, parsed from raw YYYYMMDD string | `2026-03-07` |

---

### `payments_clean`

**Source:** `payments` raw table
**Defined in:** [`database/create_views.py`](../database/create_views.py)
**Purpose:** Normalized payment records with parsed timestamps and `UNKNOWN` method removed.

| Column | Type | Description | Example |
|---|---|---|---|
| `order_guid` | STRING | FK — links to `orders_clean.order_guid` | `"abc-123"` |
| `payment_method` | STRING | Payment type (`NULL` if was `'UNKNOWN'`) | `"CREDIT"`, `"CASH"` |
| `amount` | FLOAT | Amount charged via this payment | `45.50` |
| `payment_date` | TIMESTAMP | When payment was processed; parsed from ISO 8601 with CSV fallback | `2026-03-07 00:09:00 UTC` |
| `location_id` | STRING | Denormalized restaurant location ID | `"90984"` |
| `business_date` | DATE | Business date, parsed from raw YYYYMMDD string | `2026-03-07` |

---

### `daily_sales`

**Source:** `orders` raw table (aggregated directly, pre-view)
**Defined in:** [`database/create_views.py`](../database/create_views.py)
**Purpose:** One row per `(location_id, business_date)` with key daily sales KPIs. Excludes negative `total_amount` rows.

| Column | Type | Description | Example |
|---|---|---|---|
| `location_id` | STRING | Restaurant location ID | `"90984"` |
| `business_date` | DATE | Business date | `2026-03-07` |
| `order_count` | INTEGER | `COUNT(DISTINCT order_guid)` | `142` |
| `gross_revenue` | FLOAT | `SUM(total_amount)` | `6420.00` |
| `net_revenue` | FLOAT | `SUM(total_amount - discount_amount)` | `6100.00` |
| `avg_order_value` | FLOAT | `AVG(total_amount)` | `45.21` |
| `total_tips` | FLOAT | `SUM(tip_amount)` | `890.00` |
| `total_discounts` | FLOAT | `SUM(discount_amount)` | `320.00` |
| `delivery_orders` | INTEGER | Count of orders where `order_type LIKE '%DELIVERY%'` | `58` |
| `dine_in_orders` | INTEGER | Count of orders where `order_type LIKE '%DINE%'` | `49` |
| `takeout_orders` | INTEGER | Count of all remaining orders (not delivery, not dine-in) | `35` |

> **Note:** `delivery_orders + dine_in_orders + takeout_orders` may not equal `order_count` if an order's `order_type` matches both a dine and delivery pattern (edge case).

---

### `item_performance`

**Source:** `order_items` raw table LEFT JOIN `menu_canonical_map`
**Defined in:** [`database/create_views.py`](../database/create_views.py)
**Purpose:** Item-level daily aggregates. Excludes rows with a blank `item_name`.

| Column | Type | Description | Example |
|---|---|---|---|
| `location_id` | STRING | Restaurant location ID | `"90984"` |
| `canonical_name` | STRING | Standard item key from `menu_canonical_map`; falls back to `item_name` | `"pepperoni_pizza"` |
| `display_name` | STRING | Human-readable item name; falls back to `item_name` | `"Pepperoni Pizza"` |
| `category` | STRING | Menu sales category (`NULL` if blank) | `"Pizza"` |
| `business_date` | DATE | Business date, parsed from raw YYYYMMDD string | `2026-03-07` |
| `total_qty` | INTEGER | `SUM(quantity)` | `204` |
| `total_revenue` | FLOAT | `SUM(total_price)` | `2650.92` |
| `order_count` | INTEGER | `COUNT(DISTINCT order_guid)` — orders containing this item | `143` |
| `avg_unit_price` | FLOAT | `SAFE_DIVIDE(SUM(total_price), SUM(quantity))` | `12.99` |

---

## Toast API Field Lineage

How Toast API v2 response fields map to BigQuery raw table columns. Defined in [`toast_api/field_mapping.py`](../toast_api/field_mapping.py).

### Orders — `/orders/v2/ordersBulk`

| Toast API Field | Raw Table Column | Notes |
|---|---|---|
| `guid` | `order_guid`, `order_id` | Same GUID stored in both columns |
| `openedDate` | `order_time` | ISO 8601 string |
| `diningOption.name` | `order_type` | e.g. `"Dine In"`, `"Take Out"` |
| `checks[].totalAmount` | `total_amount` | Aggregated sum across all checks |
| `checks[].amount` | `subtotal` | Pre-discount subtotal |
| `checks[].taxAmount` | `tax_amount` | |
| `checks[].tipAmount` | `tip_amount` | Not present in all API responses |
| `checks[].appliedDiscounts[].discount.amount` | `discount_amount` | Sum of all discount amounts |

### Order Items — `orders[].checks[].selections[]`

| Toast API Field | Raw Table Column | Notes |
|---|---|---|
| `displayName` | `item_name` | Raw display name from Toast |
| `salesCategory.name` | `category` | |
| `quantity` | `quantity` | |
| `price` | `total_price` | Actual line total after discounts |
| `preDiscountPrice` | `prediscount_total` | Pre-discount line total |

### Payments — `orders[].checks[].payments[]`

| Toast API Field | Raw Table Column | Notes |
|---|---|---|
| `type` | `payment_method` | e.g. `"CREDIT"`, `"CASH"` |
| `amount` | `amount` | |
| `paidDate` | `payment_date` | ISO 8601 string |

### Menu / Inventory — `/menus/v2/menus`

| Toast API Field | Raw Table Column | Notes |
|---|---|---|
| `menuGroups[].menuItems[].name` | `item_name` | |
| `menuGroups[].name` | `category` | |
| `menuGroups[].menuItems[].price` | `unit_cost` | Catalog price, not stock level |

> **Note:** The menus endpoint returns catalog/pricing data only, not live stock levels. The `current_stock` and `reorder_level` fields in the `inventory` table are sourced from CSV file imports, not the Toast API.

### Guest / Customer Data

Customer PII fields are sourced from `orders[].checks[].customer` via the `guest.pi:read` OAuth scope and stored in the `customer_orders` BigQuery table. Only checks where at least one customer field is populated are written. Voided orders and voided checks are excluded.

**Clustering:** `customer_email`, `business_date`

| Toast API Field | Raw Table Column | Mode | Notes |
|---|---|---|---|
| `order.guid` | `order_guid` | REQUIRED | Links to `orders` table |
| *(scheduler)* | `location_id` | REQUIRED | Injected from scheduler context |
| `order.businessDate` | `business_date` | REQUIRED | YYYYMMDD string |
| `checks[].customer.email` | `customer_email` | NULLABLE | |
| `checks[].customer.phone` | `customer_phone` | NULLABLE | |
| `checks[].customer.firstName` | `first_name` | NULLABLE | |
| `checks[].customer.lastName` | `last_name` | NULLABLE | |

> **Note:** Requires `guest.pi:read` OAuth scope on the Toast credentials. If the scope is not active, `customer` objects will be absent and no rows will be written. Use `summarize_customer_data()` in `scripts/test_pull.py` to verify population rate after a pull.

### Labor — Future (not yet active)

The Toast labor API (`labor:read` scope) is not yet implemented. When available, it will populate the `time_entries` table:

| Toast API Field | Raw Table Column |
|---|---|
| `employeeName` | `employee_name` |
| `jobTitle` | `job_title` |
| `inDate` | `clock_in_time` |
| `outDate` | `clock_out_time` |
| `regularHours` | `regular_hours` |
| `overtimeHours` | `overtime_hours` |
| `totalHours` | `total_hours` |
| `declaredCashTips` | `cash_tips` |
| `nonCashTips` | `non_cash_tips` |
| `wage` | `wage` |

> Currently, `time_entries` is populated from CSV/Excel file imports only.

---

## Parquet Exports

Views are exported to the `exports/` directory for local use by the Streamlit dashboard. File names mirror view names.

| File | Source View | Description |
|---|---|---|
| `orders_clean.parquet` | `orders_clean` | Full cleaned orders |
| `order_items_clean.parquet` | `order_items_clean` | Full cleaned line items |
| `payments_clean.parquet` | `payments_clean` | Full cleaned payments |
| `daily_sales.parquet` | `daily_sales` | Daily aggregates |
| `item_performance.parquet` | `item_performance` | Item-level aggregates |
| `menu_canonical_map.parquet` | `menu_canonical_map` | Item name mapping table |
| `inventory.parquet` | `inventory` (raw) | Inventory snapshots |
| `customer_orders_clean.parquet` | `customer_orders_clean` | Customer PII linked to orders (requires `guest.pi:read` scope) |
