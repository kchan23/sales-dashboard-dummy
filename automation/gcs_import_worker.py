#!/usr/bin/env python3
"""
GCS Import Worker - Automated data import pipeline.

This worker:
1. Polls GCS bucket for new files
2. Downloads new files to temp directory
3. Streams data into BigQuery
4. Cleans up temp files

Run continuously: python3 -m automation.gcs_import_worker
"""

import os
import sys
import time
import logging
import tempfile
import schedule
from pathlib import Path
from automation.storage_sync import GCSStorageSync, load_credentials_from_env
from database.import_data import DataImporter
from database.bigquery import BigQueryManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GCSImportWorker:
    """Automated worker for processing new files from GCS."""

    def __init__(self, bucket_name: str, credentials_path: str = None):
        """Initialize worker."""
        self.sync = GCSStorageSync(bucket_name, credentials_path)
        self.bucket_name = bucket_name
        self.temp_dir = None
        # We use BigQuery now so we don't need a local DB path
        self.processed_files = set() # Simple in-memory cache for this run, or query BQ for processed files

    def discover_new_files(self):
        """Find new files in GCS that haven't been processed."""
        logger.info("Discovering new files in GCS...")

        # List all files in raw/ directory
        all_files = self.sync.list_files("raw/")
        
        # In a real system, we would query BigQuery `import_log` to filter out files
        # For this prototype, we'll process all files discovered (BigQuery Importer is idempotent-ish via streaming)
        # Or we can check if we just processed them in this session
        
        new_files = []
        for gcs_path in all_files:
            # Parse location and date from path: raw/90984/20250116/OrderDetails.csv
            parts = gcs_path.split('/')
            if len(parts) >= 4:
                location_id = parts[1]
                business_date = parts[2]
                file_name = parts[3]
                
                new_files.append({
                    'gcs_path': gcs_path,
                    'location_id': location_id,
                    'business_date': business_date,
                    'file_name': file_name
                })

        logger.info(f"Found {len(new_files)} files in GCS (Importing all for now)")
        return new_files

    def download_files_for_import(self, new_files):
        """Download new files from GCS to temp directory for processing."""
        self.temp_dir = Path(tempfile.mkdtemp(prefix="gcs_import_"))
        logger.info(f"Created temp directory: {self.temp_dir}")
        num_dl = 0

        for file_info in new_files:
            # Create directory structure: data/90984/20250116/
            local_dir = self.temp_dir / "data" / file_info['location_id'] / file_info['business_date']
            local_dir.mkdir(parents=True, exist_ok=True)

            local_path = local_dir / file_info['file_name']

            # logger.info(f"Downloading {file_info['gcs_path']} to {local_path}")
            if self.sync.download_file(file_info['gcs_path'], str(local_path), show_progress=False):
                num_dl += 1

        logger.info(f"Downloaded {num_dl} files.")
        return self.temp_dir / "data"

    def run_import(self, data_dir):
        """Run import_data.py on downloaded files."""
        logger.info(f"Running import on {data_dir} -> BigQuery")

        # Change to temp directory and run import
        original_cwd = os.getcwd()
        try:
            os.chdir(self.temp_dir)

            # Run importer
            importer = DataImporter(data_dir=data_dir)
            importer.run_import()

        finally:
            os.chdir(original_cwd)

    def cleanup(self):
        """Clean up temporary files."""
        if self.temp_dir and self.temp_dir.exists():
            import shutil
            logger.info(f"Cleaning up temp directory: {self.temp_dir}")
            shutil.rmtree(self.temp_dir)

    def process_new_files(self):
        """Main processing workflow."""
        stats = {'files_found': 0, 'status': 'success', 'message': ''}
        try:
            logger.info("=" * 60)
            logger.info("Starting GCS import worker cycle")
            logger.info("=" * 60)

            # Discover new files
            new_files = self.discover_new_files()
            stats['files_found'] = len(new_files)

            if not new_files:
                logger.info("No files found to process")
                stats['message'] = "No new files found."
                return stats

            # Download files
            data_dir = self.download_files_for_import(new_files)

            # Run import
            self.run_import(data_dir)

            logger.info("✅ Import cycle completed successfully")
            stats['message'] = f"Successfully processed {len(new_files)} files."

        except Exception as e:
            logger.error(f"❌ Error in import cycle: {e}", exc_info=True)
            stats['status'] = 'error'
            stats['message'] = str(e)

        finally:
            self.cleanup()
        
        return stats


def main():
    """Main entry point for worker."""
    logger.info("GCS Import Worker starting...")

    # Load credentials
    creds = load_credentials_from_env()
    if not creds:
        logger.error("❌ No credentials found. Set up .env file.")
        sys.exit(1)

    bucket_name = creds.get('bucket_name')
    credentials_path = creds.get('credentials_path')

    if not bucket_name:
        logger.error("❌ GCS_BUCKET_NAME not set")
        sys.exit(1)

    # Create worker
    worker = GCSImportWorker(bucket_name, credentials_path)

    # Schedule to run every 5 minutes
    schedule.every(5).minutes.do(worker.process_new_files)

    # Run immediately on startup
    worker.process_new_files()

    # Keep running
    logger.info("Worker scheduled to run every 5 minutes. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(30)  # Check every 30 seconds


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n\n⚠️  Worker stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"\n❌ Worker error: {e}")
        sys.exit(1)
