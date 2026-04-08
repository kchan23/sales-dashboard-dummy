"""
DoughZone Analytics Dashboard - Automation Package

This package contains automation modules for cloud-based data ingestion:
- storage_sync: Google Cloud Storage integration
- (Week 2) gcs_import_worker: Automated import worker
- (Week 2) import_queue: Import queue management
"""

__version__ = "1.0.0"

from .storage_sync import GCSStorageSync, load_credentials_from_env, load_credentials_from_streamlit_secrets

__all__ = [
    'GCSStorageSync',
    'load_credentials_from_env',
    'load_credentials_from_streamlit_secrets'
]
