"""
Google Cloud Storage synchronization module for DoughZone Analytics Dashboard.

This module handles uploading and downloading files to/from GCS, including:
- Raw CSV/Excel data files
- SQLite database files
- Versioned backups
- Progress tracking for large files
"""

import os
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple
from google.cloud import storage
from google.cloud.exceptions import NotFound
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GCSStorageSync:
    """Manages file synchronization with Google Cloud Storage."""

    def __init__(self, bucket_name: str, credentials_path: Optional[str] = None):
        """
        Initialize GCS client.

        Args:
            bucket_name: Name of the GCS bucket
            credentials_path: Path to service account JSON key file (optional)
        """
        self.bucket_name = bucket_name

        # Initialize GCS client
        if credentials_path and os.path.exists(credentials_path):
            self.client = storage.Client.from_service_account_json(credentials_path)
            logger.info(f"Initialized GCS client with credentials from {credentials_path}")
        else:
            # Use default credentials (environment variable or gcloud auth)
            self.client = storage.Client()
            logger.info("Initialized GCS client with default credentials")

        self.bucket = self.client.bucket(bucket_name)
        logger.info(f"Connected to bucket: {bucket_name}")

    def upload_file(
        self,
        local_path: str,
        gcs_path: str,
        show_progress: bool = True
    ) -> bool:
        """
        Upload a file to GCS with progress tracking.

        Args:
            local_path: Path to local file
            gcs_path: Destination path in GCS bucket
            show_progress: Whether to show progress bar

        Returns:
            True if successful, False otherwise
        """
        try:
            local_path = Path(local_path)
            if not local_path.exists():
                logger.error(f"Local file not found: {local_path}")
                return False

            file_size = local_path.stat().st_size
            blob = self.bucket.blob(gcs_path)

            logger.info(f"Uploading {local_path} to gs://{self.bucket_name}/{gcs_path}")

            if show_progress and file_size > 1024 * 1024:  # Show progress for files > 1MB
                # Upload with progress bar
                with tqdm(total=file_size, unit='B', unit_scale=True, desc=local_path.name) as pbar:
                    def upload_callback(bytes_uploaded):
                        pbar.update(bytes_uploaded - pbar.n)

                    with open(local_path, 'rb') as f:
                        blob.upload_from_file(f, timeout=300)
            else:
                # Upload without progress bar
                blob.upload_from_filename(str(local_path), timeout=300)

            logger.info(f"Successfully uploaded to gs://{self.bucket_name}/{gcs_path}")
            return True

        except Exception as e:
            logger.error(f"Error uploading {local_path}: {e}")
            return False

    def download_file(
        self,
        gcs_path: str,
        local_path: str,
        show_progress: bool = True
    ) -> bool:
        """
        Download a file from GCS with progress tracking.

        Args:
            gcs_path: Path in GCS bucket
            local_path: Destination local path
            show_progress: Whether to show progress bar

        Returns:
            True if successful, False otherwise
        """
        try:
            blob = self.bucket.blob(gcs_path)

            if not blob.exists():
                logger.error(f"File not found in GCS: gs://{self.bucket_name}/{gcs_path}")
                return False

            local_path = Path(local_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)

            logger.info(f"Downloading gs://{self.bucket_name}/{gcs_path} to {local_path}")

            if show_progress:
                file_size = blob.size
                with tqdm(total=file_size, unit='B', unit_scale=True, desc=local_path.name) as pbar:
                    def download_callback(bytes_downloaded):
                        pbar.update(bytes_downloaded - pbar.n)

                    blob.download_to_filename(str(local_path), timeout=300)
            else:
                blob.download_to_filename(str(local_path), timeout=300)

            logger.info(f"Successfully downloaded to {local_path}")
            return True

        except Exception as e:
            logger.error(f"Error downloading {gcs_path}: {e}")
            return False

    def upload_directory(
        self,
        local_dir: str,
        gcs_prefix: str,
        pattern: str = "*"
    ) -> Tuple[int, int]:
        """
        Upload all files matching pattern from a directory to GCS.

        Args:
            local_dir: Local directory path
            gcs_prefix: GCS path prefix
            pattern: File pattern to match (e.g., "*.csv", "**/*.xlsx")

        Returns:
            Tuple of (successful_uploads, failed_uploads)
        """
        local_dir = Path(local_dir)
        if not local_dir.exists():
            logger.error(f"Directory not found: {local_dir}")
            return (0, 0)

        files = list(local_dir.glob(pattern))
        if not files:
            logger.warning(f"No files found matching pattern '{pattern}' in {local_dir}")
            return (0, 0)

        logger.info(f"Uploading {len(files)} files from {local_dir} to gs://{self.bucket_name}/{gcs_prefix}")

        successful = 0
        failed = 0

        for file_path in files:
            if file_path.is_file():
                # Preserve relative path structure
                relative_path = file_path.relative_to(local_dir)
                gcs_path = f"{gcs_prefix}/{relative_path}".replace("\\", "/")

                if self.upload_file(str(file_path), gcs_path, show_progress=False):
                    successful += 1
                else:
                    failed += 1

        logger.info(f"Upload complete: {successful} successful, {failed} failed")
        return (successful, failed)

    def list_files(self, prefix: str = "") -> List[str]:
        """
        List all files in the bucket with the given prefix.

        Args:
            prefix: GCS path prefix to filter by

        Returns:
            List of file paths in GCS
        """
        try:
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            files = [blob.name for blob in blobs]
            logger.info(f"Found {len(files)} files with prefix '{prefix}'")
            return files
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []

    def file_exists(self, gcs_path: str) -> bool:
        """
        Check if a file exists in GCS.

        Args:
            gcs_path: Path in GCS bucket

        Returns:
            True if file exists, False otherwise
        """
        try:
            blob = self.bucket.blob(gcs_path)
            return blob.exists()
        except Exception as e:
            logger.error(f"Error checking file existence: {e}")
            return False

    def get_file_metadata(self, gcs_path: str) -> Optional[dict]:
        """
        Get metadata for a file in GCS.

        Args:
            gcs_path: Path in GCS bucket

        Returns:
            Dictionary with file metadata or None if not found
        """
        try:
            blob = self.bucket.blob(gcs_path)
            if not blob.exists():
                return None

            blob.reload()
            return {
                'name': blob.name,
                'size': blob.size,
                'content_type': blob.content_type,
                'updated': blob.updated.isoformat() if blob.updated else None,
                'md5_hash': blob.md5_hash
            }
        except Exception as e:
            logger.error(f"Error getting file metadata: {e}")
            return None

    def calculate_file_hash(self, file_path: str) -> str:
        """
        Calculate SHA256 hash of a file.

        Args:
            file_path: Path to file

        Returns:
            Hex string of SHA256 hash
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def upload_database_with_backup(
        self,
        local_db_path: str,
        backup: bool = True
    ) -> bool:
        """
        Upload database file to GCS with optional versioned backup.

        Args:
            local_db_path: Path to local database file
            backup: Whether to create a timestamped backup

        Returns:
            True if successful, False otherwise
        """
        try:
            local_db_path = Path(local_db_path)
            if not local_db_path.exists():
                logger.error(f"Database file not found: {local_db_path}")
                return False

            # Upload to main location
            main_gcs_path = "databases/doughzone_analytics_latest.db"
            if not self.upload_file(str(local_db_path), main_gcs_path):
                return False

            # Create timestamped backup if requested
            if backup:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_gcs_path = f"databases/backups/doughzone_analytics_{timestamp}.db"
                self.upload_file(str(local_db_path), backup_gcs_path, show_progress=False)
                logger.info(f"Created backup: {backup_gcs_path}")

            return True

        except Exception as e:
            logger.error(f"Error uploading database: {e}")
            return False

    def download_latest_database(self, local_db_path: str) -> bool:
        """
        Download the latest database from GCS.

        Args:
            local_db_path: Destination path for database file

        Returns:
            True if successful, False otherwise
        """
        gcs_path = "databases/doughzone_analytics_latest.db"
        return self.download_file(gcs_path, local_db_path)

    def get_latest_database_metadata(self) -> Optional[dict]:
        """
        Get metadata for the latest database file.

        Returns:
            Dictionary with metadata or None if not found
        """
        gcs_path = "databases/doughzone_analytics_latest.db"
        return self.get_file_metadata(gcs_path)


def load_credentials_from_streamlit_secrets() -> Optional[dict]:
    """
    Load GCS credentials from Streamlit secrets.toml file.

    Returns:
        Dictionary with credentials or None if not found
    """
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'gcs' in st.secrets:
            return {
                'project_id': st.secrets['gcs']['project_id'],
                'bucket_name': st.secrets['gcs']['bucket_name'],
                'credentials_json': st.secrets['gcs']['credentials_json']
            }
    except Exception as e:
        logger.warning(f"Could not load from Streamlit secrets: {e}")

    return None


def load_credentials_from_env() -> Optional[dict]:
    """
    Load GCS credentials from environment variables.

    Returns:
        Dictionary with credentials or None if not found
    """
    try:
        from dotenv import load_dotenv
        load_dotenv()

        project_id = os.getenv('GCS_PROJECT_ID')
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

        if all([project_id, bucket_name, credentials_path]):
            return {
                'project_id': project_id,
                'bucket_name': bucket_name,
                'credentials_path': credentials_path
            }
    except Exception as e:
        logger.warning(f"Could not load from environment: {e}")

    return None


if __name__ == "__main__":
    # Example usage
    print("GCS Storage Sync Module")
    print("=" * 50)
    print("\nThis module provides GCS upload/download functionality.")
    print("\nExample usage:")
    print("""
from automation.storage_sync import GCSStorageSync

# Initialize
sync = GCSStorageSync(
    bucket_name='doughzone-data',
    credentials_path='path/to/service-account-key.json'
)

# Upload a file
sync.upload_file('data/90984/20250210/OrderDetails.csv',
                 'raw/90984/20250210/OrderDetails.csv')

# Download a file
sync.download_file('databases/doughzone_analytics_latest.db',
                   'doughzone_analytics.db')

# Upload entire directory
sync.upload_directory('data/90984/20250210',
                      'raw/90984/20250210',
                      pattern='*.csv')

# Upload database with backup
sync.upload_database_with_backup('doughzone_analytics.db', backup=True)
    """)
