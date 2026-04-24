"""
BigQuery module for the restaurant analytics project.
Handles warehouse connection, schema creation, and queries.
"""

import os
import logging
from typing import List, Optional, Tuple, Any
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import GoogleAPICallError
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
# Get the directory where this file is located
current_dir = Path(__file__).resolve().parent
# Go up one level to get the project root
project_root = current_dir.parent
# Load .env from project root
load_dotenv(dotenv_path=project_root / '.env')

logger = logging.getLogger(__name__)

class BigQueryManager:
    """Manages BigQuery operations for restaurant data."""

    def __init__(self, project_id: Optional[str] = None, dataset_id: str = "restaurant_analytics_demo"):
        # Try to get project ID from environment or Streamlit secrets
        self.project_id = project_id or os.getenv("GCS_PROJECT_ID")
        credentials = None

        # If not found, check Streamlit secrets
        try:
            import streamlit as st

            # Check for GCS.credentials_json section (only if running in Streamlit context)
            try:
                if "GCS" in st.secrets:
                    gcs_secrets = st.secrets["GCS"]
                    if not self.project_id:
                        self.project_id = gcs_secrets.get("project_id")

                    # Try to load credentials from secrets
                    if "credentials_json" in gcs_secrets:
                        from google.oauth2 import service_account
                        credentials_info = dict(gcs_secrets["credentials_json"])
                        
                        # Fix potential private key formatting issues (unescape newlines)
                        if "private_key" in credentials_info:
                            credentials_info["private_key"] = credentials_info["private_key"].replace("\\n", "\n")

                        credentials = service_account.Credentials.from_service_account_info(credentials_info)
                        
                        # Extract project_id from credentials if not already set
                        if not self.project_id:
                            # Try multiple possible field names for project_id
                            self.project_id = (
                                credentials_info.get("project_id") or
                                credentials_info.get("project") or
                                credentials_info.get("quota_project_id")
                            )
                        
                        if self.project_id:
                            logger.info(f"Loaded credentials for project: {self.project_id}")
                        else:
                            logger.warning("Loaded credentials but could not determine project_id from them.")
            except Exception as e:
                # Not running in Streamlit context or secrets not configured
                logger.warning(f"Could not load from Streamlit secrets: {e}")

        except ImportError:
            pass

        # Validate that we have a project ID
        if not self.project_id:
            raise ValueError(
                "BigQuery project ID not found. Please set GCS_PROJECT_ID in your .env file "
                "or configure it in Streamlit secrets (GCS.project_id or GCS.credentials_json with project_id field)."
            )

        self.dataset_id = dataset_id
        
        # Initialize client
        try:
            if credentials:
                self.client = bigquery.Client(credentials=credentials, project=self.project_id)
            else:
                # Fallback to default credentials (local env)
                self.client = bigquery.Client(project=self.project_id)
            
            self.dataset_ref = f"{self.project_id}.{self.dataset_id}"
            logger.info(f"Initialized BigQuery client for dataset: {self.dataset_ref}")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise

    def create_schema(self):
        """Create dataset and tables if they don't exist."""
        # Create dataset
        dataset = bigquery.Dataset(self.dataset_ref)
        dataset.location = "US"  # Adjust as needed
        try:
            self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {self.dataset_ref} ready")
        except Exception as e:
            logger.error(f"Error creating dataset: {e}")
            raise

        # Define schemas
        tables = {
            "orders": [
                bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"), # Using GUID as ID or constructed ID
                bigquery.SchemaField("location_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("business_date", "STRING", mode="REQUIRED"), # YYYYMMDD
                bigquery.SchemaField("order_guid", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("order_time", "STRING"),
                bigquery.SchemaField("order_type", "STRING"),
                bigquery.SchemaField("total_amount", "FLOAT"),
                bigquery.SchemaField("subtotal", "FLOAT"),
                bigquery.SchemaField("tax_amount", "FLOAT"),
                bigquery.SchemaField("tip_amount", "FLOAT"),
                bigquery.SchemaField("discount_amount", "FLOAT"),
                bigquery.SchemaField("created_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"),
            ],
            "order_items": [
                bigquery.SchemaField("order_guid", "STRING", mode="REQUIRED"), # Link via GUID
                bigquery.SchemaField("item_name", "STRING"),
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("quantity", "INTEGER"),
                bigquery.SchemaField("prediscount_total", "FLOAT"),
                bigquery.SchemaField("total_price", "FLOAT"),
                bigquery.SchemaField("location_id", "STRING"), # Denormalized for partitioning/clustering
                bigquery.SchemaField("business_date", "STRING"),
            ],
            "payments": [
                bigquery.SchemaField("order_guid", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("payment_method", "STRING"),
                bigquery.SchemaField("amount", "FLOAT"),
                bigquery.SchemaField("payment_date", "STRING"),
                bigquery.SchemaField("location_id", "STRING"),
                bigquery.SchemaField("business_date", "STRING"),
            ],
            "customer_orders": [
                bigquery.SchemaField("order_guid",     "STRING", mode="REQUIRED"),
                bigquery.SchemaField("location_id",    "STRING", mode="REQUIRED"),
                bigquery.SchemaField("business_date",  "STRING", mode="REQUIRED"),
                bigquery.SchemaField("customer_email", "STRING"),
                bigquery.SchemaField("customer_phone", "STRING"),
                bigquery.SchemaField("first_name",     "STRING"),
                bigquery.SchemaField("last_name",      "STRING"),
            ],
            "inventory": [
                bigquery.SchemaField("location_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("item_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("current_stock", "FLOAT"),
                bigquery.SchemaField("reorder_level", "FLOAT"),
                bigquery.SchemaField("unit_cost", "FLOAT"),
                bigquery.SchemaField("last_ordered", "STRING"),
                bigquery.SchemaField("snapshot_date", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("status", "STRING"), # Derived field 'good', 'low', 'critical'
            ],
            "reviews": [
                bigquery.SchemaField("review_id", "STRING"), # Generated UUID
                bigquery.SchemaField("location_id", "STRING"),
                bigquery.SchemaField("order_guid", "STRING"),
                bigquery.SchemaField("review_date", "STRING"),
                bigquery.SchemaField("rating", "INTEGER"),
                bigquery.SchemaField("review_text", "STRING"),
                bigquery.SchemaField("sentiment", "STRING"),
                bigquery.SchemaField("category", "STRING"),
            ],
            "instagram_profile_snapshots": [
                bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("account_label", "STRING"),
                bigquery.SchemaField("username", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("biography", "STRING"),
                bigquery.SchemaField("account_type", "STRING"),
                bigquery.SchemaField("media_count", "INTEGER"),
                bigquery.SchemaField("followers_count", "INTEGER"),
                bigquery.SchemaField("follows_count", "INTEGER"),
                bigquery.SchemaField("profile_picture_url", "STRING"),
                bigquery.SchemaField("local_timezone", "STRING"),
                bigquery.SchemaField("snapshot_at", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("snapshot_date", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("source_run_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"),
            ],
            "instagram_media_snapshots": [
                bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("account_label", "STRING"),
                bigquery.SchemaField("username", "STRING"),
                bigquery.SchemaField("media_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("caption", "STRING"),
                bigquery.SchemaField("media_type", "STRING"),
                bigquery.SchemaField("media_product_type", "STRING"),
                bigquery.SchemaField("permalink", "STRING"),
                bigquery.SchemaField("media_url", "STRING"),
                bigquery.SchemaField("thumbnail_url", "STRING"),
                bigquery.SchemaField("posted_at_raw", "STRING"),
                bigquery.SchemaField("posted_at_utc", "STRING"),
                bigquery.SchemaField("posted_date_utc", "STRING"),
                bigquery.SchemaField("likes", "INTEGER"),
                bigquery.SchemaField("comments_count", "INTEGER"),
                bigquery.SchemaField("views", "INTEGER"),
                bigquery.SchemaField("reach", "INTEGER"),
                bigquery.SchemaField("saved", "INTEGER"),
                bigquery.SchemaField("shares", "INTEGER"),
                bigquery.SchemaField("total_interactions", "INTEGER"),
                bigquery.SchemaField("children_json", "STRING"),
                bigquery.SchemaField("child_count", "INTEGER"),
                bigquery.SchemaField("source_run_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"),
            ],
             "time_entries": [
                bigquery.SchemaField("location_id", "STRING"),
                bigquery.SchemaField("business_date", "STRING"),
                bigquery.SchemaField("employee_name", "STRING"),
                bigquery.SchemaField("job_title", "STRING"),
                bigquery.SchemaField("clock_in_time", "STRING"),
                bigquery.SchemaField("clock_out_time", "STRING"),
                bigquery.SchemaField("total_hours", "FLOAT"),
                bigquery.SchemaField("payable_hours", "FLOAT"),
                bigquery.SchemaField("regular_hours", "FLOAT"),
                bigquery.SchemaField("overtime_hours", "FLOAT"),
                bigquery.SchemaField("cash_tips", "FLOAT"),
                bigquery.SchemaField("non_cash_tips", "FLOAT"),
                bigquery.SchemaField("total_gratuity", "FLOAT"),
                bigquery.SchemaField("total_tips", "FLOAT"),
                bigquery.SchemaField("wage", "FLOAT"),
                bigquery.SchemaField("duration_minutes", "INTEGER"),
            ],
            "import_log": [
                bigquery.SchemaField("import_id", "STRING", mode="REQUIRED"), # UUID
                bigquery.SchemaField("location_id", "STRING"),
                bigquery.SchemaField("business_date", "STRING"),
                bigquery.SchemaField("file_type", "STRING"),
                bigquery.SchemaField("file_name", "STRING"),
                bigquery.SchemaField("rows_imported", "INTEGER"),
                bigquery.SchemaField("import_timestamp", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"),
            ]
        }

        for table_name, schema in tables.items():
            table_ref = f"{self.dataset_ref}.{table_name}"
            table = bigquery.Table(table_ref, schema=schema)

            # Clustering/Partitioning for performance
            if "business_date" in [f.name for f in schema]:
                 # Note: business_date is STRING YYYYMMDD, specialized partitioning might require DATE type.
                 # For simplicity in this v1, we'll cluster by location_id and business_date
                 table.clustering_fields = ["location_id", "business_date"]

            # customer_orders: cluster by email for segmentation query performance
            if table_name == "customer_orders":
                table.clustering_fields = ["customer_email", "business_date"]

            if table_name == "instagram_profile_snapshots":
                table.clustering_fields = ["account_id", "snapshot_date"]

            if table_name == "instagram_media_snapshots":
                table.clustering_fields = ["account_id", "posted_date_utc"]

            try:
                self.client.get_table(table_ref)
                logger.info(f"Table {table_name} ready")
            except NotFound:
                try:
                    self.client.create_table(table)
                    logger.info(f"Table {table_name} created")
                except Exception as e:
                    logger.error(f"Error creating table {table_name}: {e}")

    def migrate_schema(self):
        """Add any columns missing from existing tables (safe, idempotent)."""
        migrations = [
            f"ALTER TABLE `{self.dataset_ref}.time_entries` ADD COLUMN IF NOT EXISTS job_title STRING",
            f"ALTER TABLE `{self.dataset_ref}.time_entries` ADD COLUMN IF NOT EXISTS total_hours FLOAT64",
            f"ALTER TABLE `{self.dataset_ref}.time_entries` ADD COLUMN IF NOT EXISTS payable_hours FLOAT64",
            f"ALTER TABLE `{self.dataset_ref}.time_entries` ADD COLUMN IF NOT EXISTS regular_hours FLOAT64",
            f"ALTER TABLE `{self.dataset_ref}.time_entries` ADD COLUMN IF NOT EXISTS overtime_hours FLOAT64",
            f"ALTER TABLE `{self.dataset_ref}.time_entries` ADD COLUMN IF NOT EXISTS cash_tips FLOAT64",
            f"ALTER TABLE `{self.dataset_ref}.time_entries` ADD COLUMN IF NOT EXISTS non_cash_tips FLOAT64",
            f"ALTER TABLE `{self.dataset_ref}.time_entries` ADD COLUMN IF NOT EXISTS total_gratuity FLOAT64",
            f"ALTER TABLE `{self.dataset_ref}.time_entries` ADD COLUMN IF NOT EXISTS total_tips FLOAT64",
            f"ALTER TABLE `{self.dataset_ref}.time_entries` ADD COLUMN IF NOT EXISTS wage FLOAT64",
        ]
        for stmt in migrations:
            try:
                self.client.query(stmt).result()
                logger.info(f"Migration applied: {stmt[:80]}...")
            except Exception as e:
                logger.warning(f"Migration skipped or failed: {e}")

    def execute(self, query: str, params: Optional[List[Any]] = None) -> bigquery.QueryJob:
        """
        Execute a query. 
        Note: BigQuery params are different from SQLite. 
        Use @param_name in query and passing job_config.query_parameters
        """
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = params
        
        query_job = self.client.query(query, job_config=job_config)
        return query_job

    def query_to_df(self, query: str, params: Optional[List[Any]] = None) -> pd.DataFrame:
        """Execute query and return DataFrame."""
        return self.execute(query, params).to_dataframe()

    # --- Analytics Methods (Ported from DatabaseManager) ---

    @st.cache_data(ttl=3600)
    def get_sales_summary(_self, location_ids: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        query = """
            SELECT
                FORMAT_DATE('%Y%m%d', business_date) as date,
                COUNT(*) as orders,
                SUM(total_amount) as revenue,
                AVG(total_amount) as avg_order_value,
                SUM(tip_amount) as tips,
                SUM(discount_amount) as discounts,
                SUM(tax_amount) as tax_amount,
                COUNTIF(UPPER(order_type) LIKE '%DELIVERY%') as delivery_orders,
                COUNTIF(UPPER(order_type) LIKE '%DINE%') as dine_in_orders,
                COUNTIF(UPPER(COALESCE(order_type, '')) NOT LIKE '%DELIVERY%' AND UPPER(COALESCE(order_type, '')) NOT LIKE '%DINE%') as takeout_orders
            FROM `{dataset}.orders_clean`
            WHERE location_id IN UNNEST(@location_ids)
              AND business_date BETWEEN PARSE_DATE('%Y%m%d', @start_date) AND PARSE_DATE('%Y%m%d', @end_date)
            GROUP BY business_date
            ORDER BY business_date
        """.format(dataset=_self.dataset_ref)

        params = [
            bigquery.ArrayQueryParameter("location_ids", "STRING", location_ids),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
        return _self.query_to_df(query, params)

    @st.cache_data(ttl=3600)
    def get_menu_performance(_self, location_ids: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        query = """
            SELECT
                oi.item_name as item,
                ANY_VALUE(i.category) as category,
                COUNT(*) as order_count,
                SUM(oi.total_price) as revenue,
                SAFE_DIVIDE(SUM(oi.total_price), SUM(oi.quantity)) as avg_price
            FROM `{dataset}.order_items` oi
            LEFT JOIN (
                SELECT DISTINCT item_name, location_id, category
                FROM `{dataset}.inventory`
                WHERE NULLIF(category, '') IS NOT NULL
            ) i ON oi.item_name = i.item_name AND oi.location_id = i.location_id
            WHERE oi.location_id IN UNNEST(@location_ids)
              AND oi.business_date BETWEEN @start_date AND @end_date
            GROUP BY oi.item_name
            ORDER BY revenue DESC
        """.format(dataset=_self.dataset_ref)

        params = [
            bigquery.ArrayQueryParameter("location_ids", "STRING", location_ids),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
        return _self.query_to_df(query, params)

    @st.cache_data(ttl=3600)
    def get_inventory_status(_self, location_ids: List[str], snapshot_date: str) -> pd.DataFrame:
        query = """
            SELECT
                item_name as item,
                category,
                current_stock as stock,
                reorder_level,
                unit_cost,
                last_ordered,
                CASE
                    WHEN current_stock <= 0 THEN 'critical'
                    WHEN current_stock < reorder_level THEN 'low'
                    ELSE 'good'
                END as status
            FROM `{dataset}.inventory`
            WHERE location_id IN UNNEST(@location_ids) AND snapshot_date = @snapshot_date
            ORDER BY status DESC, item_name
        """.format(dataset=_self.dataset_ref)

        params = [
            bigquery.ArrayQueryParameter("location_ids", "STRING", location_ids),
            bigquery.ScalarQueryParameter("snapshot_date", "STRING", snapshot_date),
        ]
        return _self.query_to_df(query, params)

    @st.cache_data(ttl=3600)
    def get_reviews(_self, location_ids: List[str], start_date: str = None, end_date: str = None, sentiment: str = None) -> pd.DataFrame:
        query = "SELECT * FROM `{dataset}.reviews` WHERE location_id IN UNNEST(@location_ids)".format(dataset=_self.dataset_ref)
        params = [bigquery.ArrayQueryParameter("location_ids", "STRING", location_ids)]

        if start_date:
            query += " AND review_date >= @start_date"
            params.append(bigquery.ScalarQueryParameter("start_date", "STRING", start_date))
        if end_date:
            query += " AND review_date <= @end_date"
            params.append(bigquery.ScalarQueryParameter("end_date", "STRING", end_date))
        if sentiment and sentiment != 'all':
            query += " AND sentiment = @sentiment"
            params.append(bigquery.ScalarQueryParameter("sentiment", "STRING", sentiment))

        query += " ORDER BY review_date DESC"
        return _self.query_to_df(query, params)

    @st.cache_data(ttl=3600)
    def get_labor_analytics(_self, location_ids: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        query = """
            SELECT
                business_date as date,
                employee_name,
                job_title,
                clock_in_time,
                clock_out_time,
                total_hours,
                payable_hours,
                regular_hours,
                overtime_hours,
                cash_tips,
                non_cash_tips,
                total_gratuity,
                total_tips,
                wage
            FROM `{dataset}.time_entries`
            WHERE location_id IN UNNEST(@location_ids) AND business_date BETWEEN @start_date AND @end_date
            ORDER BY business_date DESC, employee_name
        """.format(dataset=_self.dataset_ref)

        params = [
            bigquery.ArrayQueryParameter("location_ids", "STRING", location_ids),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
        return _self.query_to_df(query, params)

    @st.cache_data(ttl=3600)
    def get_daily_drivers_data(_self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return per-location daily aggregates from the daily_sales view for driver analysis.

        Fetches all locations (not filtered) so that cross-location comparisons are possible
        in the regression model. Date range is filtered to the user's selected period.
        business_date is returned as a Python date object (DATE type from the view).
        """
        query = """
            SELECT
                location_id,
                business_date,
                order_count,
                gross_revenue,
                net_revenue,
                avg_order_value,
                total_tips,
                total_discounts,
                delivery_orders,
                dine_in_orders,
                takeout_orders
            FROM `{dataset}.daily_sales`
            WHERE business_date BETWEEN PARSE_DATE('%Y%m%d', @start_date)
                                    AND PARSE_DATE('%Y%m%d', @end_date)
            ORDER BY business_date, location_id
        """.format(dataset=_self.dataset_ref)

        params = [
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
        return _self.query_to_df(query, params)

    @st.cache_data(ttl=3600)
    def get_customer_analytics(_self, location_ids: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """Return per-customer profile from customer_orders_masked joined to orders_clean.

        Each row is one unique customer_id × location_id pair. Useful for visit
        frequency, spend distributions, and repeat-vs-one-time segmentation.
        Only includes rows where customer_id is non-NULL (guest shared contact info).
        """
        query = """
            SELECT
                c.customer_id,
                c.location_id,
                COUNT(DISTINCT o.order_guid)    AS order_count,
                COUNT(DISTINCT o.business_date) AS visit_days,
                SUM(o.total_amount)             AS total_spend,
                AVG(o.total_amount)             AS avg_order,
                MIN(o.business_date)            AS first_visit,
                MAX(o.business_date)            AS last_visit
            FROM `{dataset}.customer_orders_masked` c
            JOIN `{dataset}.orders_clean` o
                ON c.order_guid = o.order_guid
            WHERE c.customer_id IS NOT NULL
              AND c.location_id IN UNNEST(@location_ids)
              AND o.business_date BETWEEN PARSE_DATE('%Y%m%d', @start_date)
                                      AND PARSE_DATE('%Y%m%d', @end_date)
            GROUP BY c.customer_id, c.location_id
        """.format(dataset=_self.dataset_ref)

        params = [
            bigquery.ArrayQueryParameter("location_ids", "STRING", location_ids),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
        return _self.query_to_df(query, params)

    def get_available_dates(self, location_ids: List[str]) -> List[str]:
        query = """
            SELECT DISTINCT business_date
            FROM `{dataset}.orders`
            WHERE location_id IN UNNEST(@location_ids)
            ORDER BY business_date DESC
        """.format(dataset=self.dataset_ref)
        params = [bigquery.ArrayQueryParameter("location_ids", "STRING", location_ids)]

        df = self.query_to_df(query, params)
        if not df.empty:
            return df['business_date'].tolist()
        return []

    def get_locations(self) -> List[str]:
        query = "SELECT DISTINCT location_id FROM `{dataset}.orders` ORDER BY location_id".format(dataset=self.dataset_ref)
        df = self.query_to_df(query)
        if not df.empty:
            return df['location_id'].tolist()
        return []

    def log_import(self, location_id: str, business_date: str, file_type: str, file_name: str, rows_imported: int):
        import uuid
        rows_to_insert = [{
            "import_id": str(uuid.uuid4()),
            "location_id": location_id,
            "business_date": business_date,
            "file_type": file_type,
            "file_name": file_name,
            "rows_imported": rows_imported
        }]
        errors = self.client.insert_rows_json(f"{self.dataset_ref}.import_log", rows_to_insert)
        if errors:
            logger.error(f"Error logging import: {errors}")

    def get_latest_import_date(self, location_id: str, source: Optional[str] = None) -> Optional[str]:
        """Get the most recent business_date imported for a location.

        Args:
            location_id: Restaurant location ID
            source: Optional file_type filter (e.g. 'TOAST_API')

        Returns:
            Latest business_date string (YYYYMMDD) or None if no imports found
        """
        query = """
            SELECT MAX(business_date) as latest_date
            FROM `{dataset}.import_log`
            WHERE location_id = @location_id
        """.format(dataset=self.dataset_ref)
        params = [bigquery.ScalarQueryParameter("location_id", "STRING", location_id)]

        if source:
            query += " AND file_type = @source"
            params.append(bigquery.ScalarQueryParameter("source", "STRING", source))

        df = self.query_to_df(query, params)
        if not df.empty and df['latest_date'].iloc[0] is not None:
            return str(df['latest_date'].iloc[0])
        return None

    def get_imported_dates(self, location_id: str, source: Optional[str] = None) -> List[str]:
        """Get all business_dates that have been imported for a location.

        Args:
            location_id: Restaurant location ID
            source: Optional file_type filter (e.g. 'TOAST_API')

        Returns:
            List of business_date strings (YYYYMMDD)
        """
        query = """
            SELECT DISTINCT business_date
            FROM `{dataset}.import_log`
            WHERE location_id = @location_id
        """.format(dataset=self.dataset_ref)
        params = [bigquery.ScalarQueryParameter("location_id", "STRING", location_id)]

        if source:
            query += " AND file_type = @source"
            params.append(bigquery.ScalarQueryParameter("source", "STRING", source))

        query += " ORDER BY business_date"
        df = self.query_to_df(query, params)
        if not df.empty:
            return df['business_date'].tolist()
        return []

    def stream_rows(self, table_name: str, rows: List[dict], batch_size: int = 500) -> int:
        """Stream JSON rows to a BigQuery table in batches.

        Args:
            table_name: Target table name (e.g. 'orders', 'order_items')
            rows: List of row dicts matching the table schema
            batch_size: Number of rows per insert request (default 500)

        Returns:
            Number of rows successfully inserted
        """
        if not rows:
            return 0

        table_ref = f"{self.dataset_ref}.{table_name}"
        total_inserted = 0

        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            errors = self.client.insert_rows_json(table_ref, batch)
            if errors:
                logger.error(f"Errors inserting batch {i//batch_size + 1} into {table_name}: {errors[:3]}...")
                return total_inserted
            total_inserted += len(batch)

        return total_inserted


# Convenience factory
def get_bq_manager() -> BigQueryManager:
    return BigQueryManager()
