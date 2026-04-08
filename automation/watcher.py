"""
Local File Watcher for DoughZone Analytics.

Monitors the 'data/' directory for new files and automatically uploads them to Google Cloud Storage.
This satisfies Requirement 1b: "Automate the sample file upload process by linking it to local folder updates."
"""

import sys
import time
import logging
import os
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from automation.storage_sync import GCSStorageSync, load_credentials_from_env

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DataWatcher")

class DataFileHandler(FileSystemEventHandler):
    """Handles file system events in the data directory."""

    def __init__(self, sync: GCSStorageSync, data_root: Path):
        self.sync = sync
        self.data_root = data_root.resolve()

    def on_created(self, event):
        if event.is_directory:
            return
        self._process_file(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            return
        self._process_file(event.src_path)

    def _process_file(self, file_path_str: str):
        """Upload file to GCS."""
        file_path = Path(file_path_str)
        
        # Ignore hidden files or temp files
        if file_path.name.startswith('.'):
            return
            
        # Only process CSV and Excel
        if file_path.suffix.lower() not in ['.csv', '.xlsx', '.xls']:
            return

        # Debounce: Wait a moment for file write to complete
        time.sleep(1)

        try:
            # Calculate relative path for GCS (e.g., data/90984/20250210/file.csv -> raw/90984/20250210/file.csv)
            relative_path = file_path.resolve().relative_to(self.data_root)
            gcs_path = f"raw/{relative_path}".replace("\\", "/")
            
            logger.info(f"Detected change in {file_path.name}. Uploading to {gcs_path}...")
            
            if self.sync.upload_file(str(file_path), gcs_path, show_progress=False):
                logger.info(f"✅ Automatically uploaded {file_path.name}")
            else:
                logger.error(f"❌ Failed to upload {file_path.name}")
                
        except ValueError:
            # File is outside data root?
            pass
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {e}")

def main():
    logger.info("Starting DoughZone Data Watcher...")
    
    # Load credentials
    creds = load_credentials_from_env()
    if not creds:
        logger.error("❌ No GCS credentials found. Please set up .env file.")
        sys.exit(1)
        
    # Initialize GCS Sync
    try:
        sync = GCSStorageSync(bucket_name=creds['bucket_name'], credentials_path=creds['credentials_path'])
        logger.info(f"Connected to GCS bucket: {creds['bucket_name']}")
    except Exception as e:
        logger.error(f"Failed to connect to GCS: {e}")
        sys.exit(1)

    # Setup Watcher
    data_dir = Path("data")
    if not data_dir.exists():
        logger.info("Data directory 'data/' not found. Creating it...")
        data_dir.mkdir(exist_ok=True)
        
    event_handler = DataFileHandler(sync, data_dir)
    observer = Observer()
    observer.schedule(event_handler, str(data_dir), recursive=True)
    
    observer.start()
    logger.info(f"👀 Watching directory: {data_dir.resolve()}")
    logger.info("Press Ctrl+C to stop")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Watcher stopped")
    
    observer.join()

if __name__ == "__main__":
    main()
