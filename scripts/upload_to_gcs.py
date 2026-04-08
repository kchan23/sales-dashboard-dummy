#!/usr/bin/env python3
"""
Upload helper script for DoughZone Analytics Dashboard.

This script helps upload CSV/Excel files to Google Cloud Storage.
It validates file structure and provides progress feedback.

Usage:
    # Upload all files for a specific location and date
    python upload_to_gcs.py --location 90984 --date 20250210

    # Upload specific files
    python upload_to_gcs.py --location 90984 --date 20250210 --files data/90984/20250210/*.csv

    # Upload entire location directory
    python upload_to_gcs.py --location 90984 --all-dates
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime
from automation.storage_sync import GCSStorageSync, load_credentials_from_env

def validate_date_format(date_str: str) -> bool:
    """Validate date is in YYYYMMDD format."""
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False

def validate_file_structure(local_dir: Path) -> tuple[bool, list]:
    """
    Validate that files exist and have expected structure.

    Returns:
        Tuple of (is_valid, list_of_files)
    """
    if not local_dir.exists():
        print(f"❌ Error: Directory not found: {local_dir}")
        return False, []

    # Look for expected file types
    csv_files = list(local_dir.glob("*.csv"))
    excel_files = list(local_dir.glob("*.xlsx")) + list(local_dir.glob("*.xls"))

    all_files = csv_files + excel_files

    if not all_files:
        print(f"⚠️  Warning: No CSV or Excel files found in {local_dir}")
        return False, []

    print(f"✓ Found {len(csv_files)} CSV files and {len(excel_files)} Excel files")
    return True, all_files

def upload_date_directory(sync: GCSStorageSync, location_id: str, date_str: str, data_root: Path) -> bool:
    """Upload all files for a specific location and date."""

    print(f"\n📤 Uploading data for Location {location_id}, Date {date_str}")
    print("=" * 60)

    # Validate date format
    if not validate_date_format(date_str):
        print(f"❌ Error: Invalid date format '{date_str}'. Expected YYYYMMDD (e.g., 20250210)")
        return False

    # Check local directory
    local_dir = data_root / location_id / date_str
    is_valid, files = validate_file_structure(local_dir)

    if not is_valid:
        return False

    # Show file list
    print("\nFiles to upload:")
    for f in files:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  • {f.name} ({size_mb:.2f} MB)")

    # Confirm upload
    confirm = input(f"\n Upload {len(files)} files to GCS? [y/N]: ").strip().lower()
    if confirm != 'y':
        print("Upload cancelled.")
        return False

    # Upload files
    print("\nUploading...")
    gcs_prefix = f"raw/{location_id}/{date_str}"
    successful, failed = sync.upload_directory(str(local_dir), gcs_prefix, pattern="*")

    # Summary
    print("\n" + "=" * 60)
    if failed == 0:
        print(f"✅ Success! Uploaded {successful} files to gs://{sync.bucket_name}/{gcs_prefix}")
        return True
    else:
        print(f"⚠️  Partial success: {successful} uploaded, {failed} failed")
        return False

def upload_all_dates(sync: GCSStorageSync, location_id: str, data_root: Path) -> bool:
    """Upload all date directories for a location."""

    print(f"\n📤 Uploading all data for Location {location_id}")
    print("=" * 60)

    location_dir = data_root / location_id
    if not location_dir.exists():
        print(f"❌ Error: Location directory not found: {location_dir}")
        return False

    # Find all date directories
    date_dirs = [d for d in location_dir.iterdir() if d.is_dir() and d.name.isdigit()]

    if not date_dirs:
        print(f"❌ Error: No date directories found in {location_dir}")
        return False

    date_dirs.sort()
    print(f"\nFound {len(date_dirs)} date directories:")
    for d in date_dirs:
        print(f"  • {d.name}")

    confirm = input(f"\nUpload all {len(date_dirs)} dates? [y/N]: ").strip().lower()
    if confirm != 'y':
        print("Upload cancelled.")
        return False

    # Upload each date
    total_successful = 0
    total_failed = 0

    for date_dir in date_dirs:
        date_str = date_dir.name
        print(f"\n📅 Processing {date_str}...")

        gcs_prefix = f"raw/{location_id}/{date_str}"
        successful, failed = sync.upload_directory(str(date_dir), gcs_prefix, pattern="*")

        total_successful += successful
        total_failed += failed

    # Summary
    print("\n" + "=" * 60)
    print(f"✅ Upload complete!")
    print(f"   Total files uploaded: {total_successful}")
    if total_failed > 0:
        print(f"   Failed: {total_failed}")

    return total_failed == 0

def main():
    parser = argparse.ArgumentParser(
        description="Upload DoughZone data files to Google Cloud Storage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload all files for a specific date
  python upload_to_gcs.py --location 90984 --date 20250210

  # Upload all dates for a location
  python upload_to_gcs.py --location 90984 --all-dates

  # Specify custom data directory
  python upload_to_gcs.py --location 90984 --date 20250210 --data-dir /path/to/data

  # Use custom credentials file
  python upload_to_gcs.py --location 90984 --date 20250210 --credentials ./service-account-key.json
        """
    )

    parser.add_argument('--location', required=True, help='Location ID (e.g., 90984)')
    parser.add_argument('--date', help='Date in YYYYMMDD format (e.g., 20250210)')
    parser.add_argument('--all-dates', action='store_true', help='Upload all dates for the location')
    parser.add_argument('--data-dir', default='data', help='Root data directory (default: data)')
    parser.add_argument('--bucket', help='GCS bucket name (default: from env/secrets)')
    parser.add_argument('--credentials', help='Path to service account JSON key file')

    args = parser.parse_args()

    # Validate arguments
    if not args.date and not args.all_dates:
        parser.error("Must specify either --date or --all-dates")

    if args.date and args.all_dates:
        parser.error("Cannot specify both --date and --all-dates")

    # Load credentials
    print("🔐 Loading GCS credentials...")

    creds = load_credentials_from_env()
    if not creds and not args.credentials:
        print("❌ Error: No credentials found!")
        print("\nPlease either:")
        print("  1. Set environment variables (GCS_BUCKET_NAME, GOOGLE_APPLICATION_CREDENTIALS)")
        print("  2. Use --credentials to specify service account key file")
        print("  3. Configure gcloud auth application-default login")
        sys.exit(1)

    bucket_name = args.bucket or (creds.get('bucket_name') if creds else None)
    credentials_path = args.credentials or (creds.get('credentials_path') if creds else None)

    if not bucket_name:
        print("❌ Error: No bucket name specified!")
        print("Set GCS_BUCKET_NAME environment variable or use --bucket")
        sys.exit(1)

    # Initialize GCS sync
    try:
        sync = GCSStorageSync(bucket_name, credentials_path)
        print(f"✓ Connected to bucket: {bucket_name}")
    except Exception as e:
        print(f"❌ Error connecting to GCS: {e}")
        sys.exit(1)

    # Get data root
    data_root = Path(args.data_dir)
    if not data_root.exists():
        print(f"❌ Error: Data directory not found: {data_root}")
        sys.exit(1)

    # Upload based on mode
    try:
        if args.all_dates:
            success = upload_all_dates(sync, args.location, data_root)
        else:
            success = upload_date_directory(sync, args.location, args.date, data_root)

        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Upload interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error during upload: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
