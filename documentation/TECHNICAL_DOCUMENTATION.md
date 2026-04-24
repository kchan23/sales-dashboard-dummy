# Restaurant Analytics Demo
## Technical Documentation

Restaurant Analytics Capstone Project  
California State Polytechnic University, Pomona  
Generated: January 23, 2026

---

## Table of Contents
1. Executive Summary  
2. System Architecture & Design  
3. Module Documentation  
4. Workflow & Data Pipeline  
5. AI Prompt Engineering  
6. API Integrations  
7. Maintenance Guide  
8. Testing & Evaluation Results  
9. Demo Walkthrough  
10. Troubleshooting Guide  
11. Appendix  

---

# 1. Executive Summary

## 1.1 Project Overview
This Streamlit-based analytics application supports restaurant operations analysis. Presentation mode uses synthetic demo data and avoids live system dependencies.

## 1.2 Key Features
- Interactive sales analytics (revenue, orders, tips, discounts)
- Menu performance insights (top items by revenue or count)
- Inventory status tracking with low/critical thresholds
- Location and date range filtering
- Presentation-safe demo mode with synthetic data
- Optional live Toast and Instagram ingestion paths for non-demo use

## 1.3 Technology Stack
| Component | Technology |
| --- | --- |
| Frontend | Streamlit |
| Backend | Python 3.8+ |
| Default Storage | Local parquet demo files |
| Optional Live Storage | Google BigQuery |
| Optional Live Cloud Storage | Google Cloud Storage (GCS) |
| Visualization | Plotly |
| Data Processing | Pandas, OpenPyXL |
| Scheduling | schedule |

## 1.4 Differences from Reference Documentation
This system differs from the Social Media Analytics Dashboard reference in several important ways:
- **Data domain**: restaurant POS data (not social media engagement data)
- **Storage**: BigQuery dataset (not SQLite)
- **AI features**: rule-based natural language to SQL (no LLM or image analysis)
- **APIs**: Google BigQuery and GCS (no Vision/Imagen/OpenRouter)

---

# 2. System Architecture & Design

## 2.1 High-Level Architecture
```
┌────────────────────────────┐
│        Streamlit UI         │
│          app.py             │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│     BigQueryManager          │
│   database/bigquery.py       │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│     BigQuery Dataset         │
│ restaurant_analytics_demo    │
└────────────────────────────┘
              ▲
              │
┌─────────────┴──────────────┐
│    Data Import Pipelines    │
│ import_data.py / GCS Worker │
└────────────────────────────┘
              ▲
              │
┌─────────────┴──────────────┐
│   Raw CSV/XLSX Files         │
│ data/ or gs://.../raw/...    │
└────────────────────────────┘
```

## 2.2 Application Layers
- **UI Layer**: `app.py` renders the dashboard and defaults to demo mode.
- **Default Data Access Layer**: `database/demo_db.py` serves bundled synthetic parquet data.
- **Optional Live Data Access Layer**: `database/bigquery.py` owns dataset creation and analytics queries.
- **Optional Live Integration Layer**: `integrations/toast_api/` and `integrations/instagram_api/` handle external ingestion.

## 2.3 Storage Model
- **BigQuery Dataset**: `restaurant_analytics_demo`
- **Tables**: `orders`, `order_items`, `payments`, `inventory`, `reviews`, `time_entries`, `import_log`, `instagram_profile_snapshots`, `instagram_media_snapshots`
- **Clustering**: `location_id`, `business_date` (per table with business_date)

## 2.4 Security & Credentials
- Default demo mode requires no external credentials.
- Live mode uses GCP service account credentials and API credentials.
- Supports `.env` and Streamlit secrets (`GCS` block).

---

# 3. Module Documentation

## 3.1 `app.py`
Streamlit dashboard entry point:
- Initializes BigQuery connection via `get_bq_manager()`
- Sidebar controls for location/date selection
- Pages: Ask Data, Overview, Sales Analytics, Menu Performance, Inventory
- Manual “Sync Now” button triggers GCS import worker

## 3.2 `database/bigquery.py`
BigQuery integration:
- Creates dataset and tables (`create_schema`)
- Query helpers: `get_sales_summary`, `get_menu_performance`, `get_inventory_status`, `get_reviews`
- Helpers: `get_locations`, `get_available_dates`, `log_import`

## 3.3 `import_data.py`
CSV/Excel to BigQuery streaming importer:
- Scans `data/[LOCATION_ID]/[YYYYMMDD]/`
- Detects file types by filename
- Normalizes columns and streams JSON rows to BigQuery

## 3.4 `automation/gcs_import_worker.py`
Automated data ingestion:
- Polls GCS bucket for new files
- Downloads into a temp `data/` structure
- Runs the same importer (`import_data.py`)
- Scheduled every 5 minutes via `schedule`

## 3.5 `automation/storage_sync.py`
GCS file synchronization:
- Upload/download helpers
- Progress display for large files
- List files and metadata support

## 3.6 `upload_to_gcs.py`
CLI helper for uploading local data to GCS:
- Validates date format
- Uploads files to `gs://<bucket>/raw/<location>/<date>/`

## 3.7 `query_generator.py`
Natural-language to SQL rules:
- Keyword matching for trends, totals, top items, inventory, orders, averages
- Generates parameterized BigQuery SQL
- Used in “Ask Data a Question”

## 3.8 `setup_check.py`
Environment verifier:
- Confirms Python version and required files
- Checks dependencies and credentials

## 3.9 `test_gcs_setup.py`
Quick GCS connection test for credentials and bucket access.

---

# 4. Workflow & Data Pipeline

## 4.1 Local Import Workflow
1. Place files in `data/[LOCATION_ID]/[YYYYMMDD]/`
2. Run `python import_data.py`
3. BigQuery tables are created (if missing) and data is streamed

## 4.2 GCS Import Workflow
1. Upload files to GCS with `upload_to_gcs.py`
2. Run `python -m automation.gcs_import_worker`
3. Worker downloads into a temp `data/` tree and imports into BigQuery

## 4.3 Runtime Query Flow
1. User selects location and date range
2. `BigQueryManager` executes parameterized SQL
3. Results are rendered in Plotly charts and tables

## 4.4 Data Structure (Required)
```
data/
└── [LOCATION_ID]/
    └── [YYYYMMDD]/
        ├── OrderDetails.csv
        ├── PaymentDetails.csv
        ├── ItemSelectionDetails.csv
        ├── TimeEntries.csv
        ├── CheckDetails.csv
        └── *.xlsx (AllItemsReport, AccountingReport, etc.)
```

---

# 5. AI Prompt Engineering

## 5.1 Approach
The “Ask Data a Question” feature uses a **dual-mode** query pipeline:
1. **LLM mode (primary)**: `llm_query_generator.py` uses OpenRouter to call a GPT-class model. It detects ambiguous queries via `AmbiguityDetector`, requests clarification when needed, and generates parameterized BigQuery SQL guided by 18 few-shot examples in `config/prompts.py`.
2. **Rule-based fallback**: `query_generator.py` handles queries via keyword-to-SQL templates if the LLM is unavailable (no API key, network error, etc.).

## 5.2 Supported Query Types
The LLM can handle open-ended natural language queries. Supported patterns include:
- Trends (daily/weekly/monthly revenue, orders)
- Totals (revenue, tips, discounts, tax)
- Top items (by count or revenue)
- Inventory status (snapshot)
- Order counts, averages, and breakdowns by order type
- Time-of-day and day-of-week analysis

## 5.3 SQL Validation
All generated SQL passes through `query/validator.py` before execution (four stages):
- **Security check**: Blocks DDL/DML (DROP, DELETE, INSERT, UPDATE, ALTER, CREATE, TRUNCATE)
- **Privacy check**: Blocks references to `customer_orders`/`customer_orders_clean` (raw PII tables); blocks bare `order_guid`/`order_id` as SELECT output columns (aggregates permitted)
- **Parameter check**: Ensures required `@location_id`, `@start_date`, `@end_date` are present
- **BigQuery dry-run**: Validates syntax and schema references against the live dataset

---

# 6. API Integrations

## 6.1 Google BigQuery
- Query execution and storage of all analytics data
- Parameterized SQL with `ScalarQueryParameter`

## 6.2 Google Cloud Storage
- Raw data ingestion and sync
- Bucket structure: `raw/<location>/<date>/`

## 6.3 Streamlit
- UI rendering
- Cached DB connection (`@st.cache_resource`)
- Optional secrets for credentials

## 6.4 Plotly and Pandas
- Plotly for charts
- Pandas for data manipulation and CSV/Excel parsing

---

# 7. Maintenance Guide for Doughzone

## 7.1 Credentials and Environment
- Set `GCS_PROJECT_ID`, `GCS_BUCKET_NAME`, `GOOGLE_APPLICATION_CREDENTIALS`
- Prefer `.env` for local and Streamlit secrets for cloud

## 7.2 Routine Data Updates
- Upload new files to GCS
- Run GCS import worker (manual or scheduled)

## 7.3 Schema Changes
- Update `create_schema()` in `database/bigquery.py`
- Adjust import handlers in `import_data.py`
- Update dashboard queries in `app.py`

## 7.4 Known Documentation Drift
Some older docs reference SQLite (`doughzone_analytics.db`). Current code uses **BigQuery**; treat SQLite notes as legacy.

---

# 8. Testing & Evaluation Results

## 8.1 Setup Verification
Run:
```
python setup_check.py
```
Validates dependencies and credentials.

## 8.2 GCS Connectivity Test
Run:
```
python test_gcs_setup.py
```

## 8.3 Manual Verification Checklist
- Dashboard loads with locations and date ranges
- Sales summary metrics are populated
- “Ask Data a Question” returns results
- Inventory page shows status labels

## 8.4 Current Testing Gaps
- No automated unit tests
- No integration test suite for BigQuery
- Manual verification is required for deployment

---

# 9. Demo Walkthrough

## 9.1 Local Demo
1. `pip install -r requirements.txt`
2. `python import_data.py`
3. `streamlit run app.py`

## 9.2 GCS Demo
1. Upload data: `python upload_to_gcs.py --location 90984 --date 20250210`
2. Start worker: `python -m automation.gcs_import_worker`
3. In the app, click “Sync Now”

## 9.3 Ask Data Demo
Example questions:
- “Show me daily revenue trends”
- “What are the top 10 items by revenue?”
- “How many orders last week?”

---

# 10. Troubleshooting Guide

## 10.1 Credentials Errors
Symptoms: “Could not automatically determine credentials”
Fix:
- Ensure `GOOGLE_APPLICATION_CREDENTIALS` points to the JSON key
- Confirm `.env` is loaded or Streamlit secrets are set

## 10.2 No Locations Found
Symptoms: “No locations found in the database”
Fix:
- Confirm data import ran successfully
- Check that `orders` table contains data

## 10.3 BigQuery Permissions
Symptoms: “Access Denied” or “Permission denied”
Fix:
- Service account must have BigQuery Data Editor and Job User roles

## 10.4 Import Worker Errors
Symptoms: Worker reports errors downloading or inserting
Fix:
- Verify bucket name and raw/ folder structure
- Confirm GCS key has Storage Admin or object read permissions

## 10.5 Excel Read Failures
Symptoms: pandas errors reading `.xlsx`
Fix:
- Install `openpyxl`
- Verify file format is not corrupted

---

# 11. Appendix

## 11.1 Core Tables (BigQuery)
See [`data_dictionary.md`](data_dictionary.md) for the full schema including raw table definitions, derived view columns, field types, example values, and Toast API field lineage.

## 11.2 Environment Variables
```
GCS_PROJECT_ID=...
GCS_BUCKET_NAME=...
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

## 11.3 Command Reference
```
pip install -r requirements.txt
python import_data.py
python -m automation.gcs_import_worker
streamlit run app.py
python setup_check.py
python test_gcs_setup.py
```

## 11.4 File Structure
```
dashboard-app/
├── app.py
├── import_data.py
├── query_generator.py
├── automation/
│   ├── gcs_import_worker.py
│   └── storage_sync.py
├── database/
│   └── bigquery.py
├── documentation/
│   └── TECHNICAL_DOCUMENTATION.md
└── data/
    └── [LOCATION_ID]/[YYYYMMDD]/
```
