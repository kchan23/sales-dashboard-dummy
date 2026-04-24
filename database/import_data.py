"""
Data import script for DoughZone Analytics Dashboard.
Reads CSV and Excel files from the data/ directory and streams them into BigQuery.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
import logging
from database.bigquery import BigQueryManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path("data")


class DataImporter:
    """Imports restaurant data from CSV/Excel files into BigQuery."""

    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
        self.bq = BigQueryManager()
        self.import_stats = {
            'locations': 0,
            'dates': 0,
            'files': 0,
            'rows': 0,
            'errors': 0,
        }

    def close(self):
        """Close database connection."""
        pass # BigQuery client doesn't need explicit close

    def run_import(self):
        """Run full import process."""
        try:
            logger.info(f"Starting data import from {self.data_dir}")

            # Ensure schema exists using BigQueryManager
            logger.info("Verifying BigQuery schema...")
            self.bq.create_schema()

            # Load valid location GUIDs from the scheduler-maintained cache
            loc_path = Path(__file__).parent.parent / "integrations" / "toast_api" / "location_names.json"
            if loc_path.exists():
                import json
                valid_location_ids = set(json.loads(loc_path.read_text()).keys())
                logger.info(f"Valid location IDs loaded: {valid_location_ids}")
            else:
                valid_location_ids = set()
                logger.warning("location_names.json not found — run scheduler first to populate valid GUIDs; all locations will be imported")

            # Iterate through location directories
            if not self.data_dir.exists():
                logger.warning(f"Data directory not found: {self.data_dir}")
                return

            for location_dir in sorted(self.data_dir.iterdir()):
                if not location_dir.is_dir():
                    continue

                location_id = location_dir.name

                if valid_location_ids and location_id not in valid_location_ids:
                    logger.warning(f"Skipping unknown location_id '{location_id}' — not in location_names.json")
                    continue

                logger.info(f"Processing location: {location_id}")
                self.import_stats['locations'] += 1

                # Iterate through date directories
                for date_dir in sorted(location_dir.iterdir()):
                    if not date_dir.is_dir():
                        continue

                    business_date = date_dir.name
                    logger.info(f"  Processing date: {business_date}")
                    self.import_stats['dates'] += 1

                    # Process files in this date directory
                    self._process_date_directory(location_id, business_date, date_dir)

            logger.info(f"Import complete. Stats: {self.import_stats}")

        except Exception as e:
            logger.error(f"Import failed: {e}", exc_info=True)
            raise
    
    def _stream_to_bq(self, table_name: str, rows: List[Dict[str, Any]]) -> int:
        """Stream JSON rows to BigQuery."""
        if not rows:
            return 0
        
        table_ref = f"{self.bq.dataset_ref}.{table_name}"
        errors = self.bq.client.insert_rows_json(table_ref, rows)
        if errors:
            logger.error(f"Encountered {len(errors)} errors inserting into {table_name}: {errors[:5]}...")
            # Simple retry or log could be added here
            return 0 # Or partial
        return len(rows)

    def _process_date_directory(self, location_id: str, business_date: str, date_dir: Path):
        """Process all CSV and Excel files in a date directory."""
        try:
            # Discover files
            csv_files = list(date_dir.glob("*.csv"))
            excel_files = list(date_dir.glob("*.xlsx"))

            for file_path in csv_files:
                self._process_csv_file(location_id, business_date, file_path)

            for file_path in excel_files:
                self._process_excel_file(location_id, business_date, file_path)

        except Exception as e:
            logger.error(f"Error in directory {date_dir}: {e}")
            self.import_stats['errors'] += 1

    def _process_csv_file(self, location_id: str, business_date: str, file_path: Path):
        """Process a CSV file based on its name."""
        file_name = file_path.name
        # logger.info(f"    Importing CSV: {file_name}")

        try:
            df = pd.read_csv(file_path)
            rows_imported = 0

            if "OrderDetails" in file_name:
                rows_imported = self._import_order_details(location_id, business_date, df)
            elif "PaymentDetails" in file_name:
                rows_imported = self._import_payment_details(location_id, business_date, df)
            elif "ItemSelectionDetails" in file_name:
                rows_imported = self._import_item_selection(location_id, business_date, df)
            elif "CheckDetails" in file_name:
                rows_imported = self._import_check_details(location_id, business_date, df)
            elif "TimeEntries" in file_name:
                rows_imported = self._import_time_entries(location_id, business_date, df)
            elif "AllItemsReport" in file_name:
                rows_imported = self._import_all_items_report_csv(location_id, business_date, df)

            if rows_imported > 0:
                self.bq.log_import(location_id, business_date, "CSV", file_name, rows_imported)
                self.import_stats['files'] += 1
                self.import_stats['rows'] += rows_imported
                # logger.info(f"      Imported {rows_imported} rows")

            return rows_imported

        except Exception as e:
            logger.error(f"Error processing CSV {file_name}: {e}")
            self.import_stats['errors'] += 1
            return 0

    def _process_excel_file(self, location_id: str, business_date: str, file_path: Path):
        """Process an Excel file based on its name."""
        file_name = file_path.name
        # logger.info(f"    Importing Excel: {file_name}")

        try:
            # Simple header check/read. Actual import logic kept simple as per original
            if "AccountingReport" in file_name:
                pass 
            elif "AllItemsReport" in file_name:
                rows_imported = self._import_all_items_report(location_id, business_date, file_path)
                if rows_imported:
                    self.bq.log_import(location_id, business_date, "XLSX", file_name, rows_imported)
                    self.import_stats['files'] += 1
                    self.import_stats['rows'] += rows_imported

        except Exception as e:
            logger.error(f"Error processing Excel {file_name}: {e}")
            self.import_stats['errors'] += 1

    # --- Import Handlers (Refactored for BigQuery JSON streaming) ---

    def _import_order_details(self, location_id: str, business_date: str, df: pd.DataFrame) -> int:
        if df.empty: return 0
        
        rows = []
        for _, row in df.iterrows():
            try:
                order_guid = str(row.get('Order Id', row.get('order_id', ''))) if pd.notna(row.get('Order Id')) else None
                if not order_guid: continue

                rows.append({
                    "location_id": location_id,
                    "business_date": business_date,
                    "order_guid": order_guid,
                    "order_id": order_guid, # Using GUID as ID
                    "order_time": str(row.get('Opened', '')) if pd.notna(row.get('Opened')) else None,
                    "order_type": str(row.get('Dining Options', row.get('Service', ''))) if pd.notna(row.get('Dining Options')) else 'UNKNOWN',
                    "total_amount": float(row.get('Total', 0)) if pd.notna(row.get('Total')) else 0.0,
                    "subtotal": float(row.get('Amount', 0)) if pd.notna(row.get('Amount')) else 0.0,
                    "tax_amount": float(row.get('Tax', 0)) if pd.notna(row.get('Tax')) else 0.0,
                    "tip_amount": float(row.get('Tip', 0)) if pd.notna(row.get('Tip')) else 0.0,
                    "discount_amount": float(row.get('Discount Amount', 0)) if pd.notna(row.get('Discount Amount')) else 0.0,
                })
            except Exception:
                continue
        
        return self._stream_to_bq("orders", rows)

    def _import_payment_details(self, location_id: str, business_date: str, df: pd.DataFrame) -> int:
        if df.empty: return 0
        rows = []
        for _, row in df.iterrows():
            try:
                order_guid = str(row.get('order_guid', row.get('order_id', ''))) if pd.notna(row.get('order_guid')) else None
                if not order_guid: continue

                rows.append({
                    "order_guid": order_guid,
                    "payment_method": str(row.get('payment_method', '')) if pd.notna(row.get('payment_method')) else 'UNKNOWN',
                    "amount": float(row.get('amount', 0)) if pd.notna(row.get('amount')) else 0.0,
                    "payment_date": str(row.get('payment_date', business_date)) if pd.notna(row.get('payment_date')) else business_date,
                    "location_id": location_id,
                    "business_date": business_date
                })
            except Exception:
               continue
        return self._stream_to_bq("payments", rows)

    def _import_item_selection(self, location_id: str, business_date: str, df: pd.DataFrame) -> int:
        if df.empty: return 0
        rows = []
        for _, row in df.iterrows():
            try:
                order_guid = str(row.get('Order Id')) if pd.notna(row.get('Order Id')) else None
                if not order_guid: continue

                rows.append({
                    "order_guid": order_guid,
                    "item_name": str(row.get('Menu Item', '')) if pd.notna(row.get('Menu Item')) else '',
                    "category": str(row.get('Sales Category', '')) if pd.notna(row.get('Sales Category')) else '',
                    "quantity": int(row.get('Qty', 1)) if pd.notna(row.get('Qty')) else 1,
                    "prediscount_total": float(row.get('Gross Price', 0)) if pd.notna(row.get('Gross Price')) else 0.0,
                    "total_price": float(row.get('Net Price', 0)) if pd.notna(row.get('Net Price')) else 0.0,
                    "location_id": location_id,
                    "business_date": business_date
                })
            except Exception:
                continue
        return self._stream_to_bq("order_items", rows)

    def _import_check_details(self, location_id: str, business_date: str, df: pd.DataFrame) -> int:
        return self._import_order_details(location_id, business_date, df)

    def _import_all_items_report_csv(self, location_id: str, business_date: str, df: pd.DataFrame) -> int:
        if df.empty: return 0
        rows = []
        for _, row in df.iterrows():
            try:
                item_name = str(row.get('Menu Item', '')) if pd.notna(row.get('Menu Item')) else ''
                if not item_name: continue
                
                rows.append({
                    "location_id": location_id,
                    "item_name": item_name,
                    "category": str(row.get('Menu Group', '')) if pd.notna(row.get('Menu Group')) else '',
                    "current_stock": float(row.get('Item Qty', 0)) if pd.notna(row.get('Item Qty')) else 0.0,
                    "reorder_level": max(10, (float(row.get('Item Qty', 0)) or 0) * 0.1),
                    "unit_cost": float(row.get('Avg Price', 0)) if pd.notna(row.get('Avg Price')) else 0.0,
                    "snapshot_date": business_date,
                    "status": "good" # Simple default
                })
            except Exception:
                continue
        return self._stream_to_bq("inventory", rows)

    def _import_time_entries(self, location_id: str, business_date: str, df: pd.DataFrame) -> int:
        if df.empty: return 0
        rows = []
        for _, row in df.iterrows():
            try:
                employee = str(row.get('Employee', '')).strip() if pd.notna(row.get('Employee')) else ''
                if not employee:
                    continue
                rows.append({
                    "location_id": location_id,
                    "business_date": business_date,
                    "employee_name": employee,
                    "job_title": str(row.get('Job Title', '')) if pd.notna(row.get('Job Title')) else '',
                    "clock_in_time": str(row.get('In Date', '')) if pd.notna(row.get('In Date')) else None,
                    "clock_out_time": str(row.get('Out Date', '')) if pd.notna(row.get('Out Date')) else None,
                    "total_hours": float(row.get('Total Hours', 0)) if pd.notna(row.get('Total Hours')) else 0.0,
                    "payable_hours": float(row.get('Payable Hours', 0)) if pd.notna(row.get('Payable Hours')) else 0.0,
                    "regular_hours": float(row.get('Regular Hours', 0)) if pd.notna(row.get('Regular Hours')) else 0.0,
                    "overtime_hours": float(row.get('Overtime Hours', 0)) if pd.notna(row.get('Overtime Hours')) else 0.0,
                    "cash_tips": float(row.get('Cash Tips Declared', 0)) if pd.notna(row.get('Cash Tips Declared')) else 0.0,
                    "non_cash_tips": float(row.get('Non Cash Tips', 0)) if pd.notna(row.get('Non Cash Tips')) else 0.0,
                    "total_gratuity": float(row.get('Total Gratuity', 0)) if pd.notna(row.get('Total Gratuity')) else 0.0,
                    "total_tips": float(row.get('Total Tips', 0)) if pd.notna(row.get('Total Tips')) else 0.0,
                    "wage": float(row.get('Wage', 0)) if pd.notna(row.get('Wage')) else 0.0,
                    "duration_minutes": int(float(row.get('Total Hours', 0)) * 60) if pd.notna(row.get('Total Hours')) else 0,
                })
            except Exception:
                continue
        return self._stream_to_bq("time_entries", rows)

    def _import_all_items_report(self, location_id: str, business_date: str, file_path: Path) -> int:
        if not file_path.exists(): return 0
        try:
            df = pd.read_csv(file_path)
            return self._import_all_items_report_csv(location_id, business_date, df)
        except Exception:
            return 0


def main():
    """Run the import process."""
    importer = DataImporter()
    importer.run_import()

    print("\n" + "="*50)
    print("IMPORT COMPLETE")
    print(f"Stats: {importer.import_stats}")
    print("="*50)


if __name__ == "__main__":
    main()
