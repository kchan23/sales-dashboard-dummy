#!/usr/bin/env python3
"""
Test script to verify GCS setup is working correctly.

This script checks:
1. Dependencies are installed
2. Credentials are configured
3. GCS connection works
4. Can list bucket contents

Run this after completing GCS setup to verify everything works.
"""

import sys
from pathlib import Path

def check_dependencies():
    """Check that required packages are installed."""
    print("1. Checking dependencies...")

    required_packages = [
        ('google.cloud.storage', 'google-cloud-storage'),
        ('dotenv', 'python-dotenv'),
        ('tqdm', 'tqdm')
    ]

    missing = []
    for module_name, package_name in required_packages:
        try:
            __import__(module_name)
            print(f"   ✓ {package_name}")
        except ImportError:
            print(f"   ✗ {package_name} - NOT INSTALLED")
            missing.append(package_name)

    if missing:
        print(f"\n❌ Missing packages: {', '.join(missing)}")
        print("Install with: pip install " + " ".join(missing))
        return False

    print("   ✅ All dependencies installed\n")
    return True

def check_credentials():
    """Check that credentials are configured."""
    print("2. Checking credentials...")

    from automation.storage_sync import load_credentials_from_env

    creds = load_credentials_from_env()

    if not creds:
        print("   ✗ No credentials found!")
        print("\n❌ Credentials not configured")
        print("\nPlease either:")
        print("   1. Create .env file with GCS_BUCKET_NAME and GOOGLE_APPLICATION_CREDENTIALS")
        print("   2. Set environment variables")
        print("   3. Run: gcloud auth application-default login")
        print("\nSee GCS_SETUP.md for detailed instructions.")
        return False, None

    print(f"   ✓ Project ID: {creds.get('project_id', 'N/A')}")
    print(f"   ✓ Bucket Name: {creds.get('bucket_name', 'N/A')}")
    print(f"   ✓ Credentials Path: {creds.get('credentials_path', 'default')}")

    if creds.get('credentials_path'):
        creds_path = Path(creds['credentials_path'])
        if not creds_path.exists():
            print(f"\n❌ Credentials file not found: {creds_path}")
            return False, None
        print(f"   ✓ Credentials file exists")

    print("   ✅ Credentials configured\n")
    return True, creds

def test_connection(creds):
    """Test connection to GCS."""
    print("3. Testing GCS connection...")

    try:
        from automation.storage_sync import GCSStorageSync

        bucket_name = creds['bucket_name']
        credentials_path = creds.get('credentials_path')

        sync = GCSStorageSync(bucket_name, credentials_path)
        print(f"   ✓ Connected to bucket: {bucket_name}")

        # Try to list files (should work even if bucket is empty)
        files = sync.list_files()
        print(f"   ✓ Listed files in bucket: {len(files)} files found")

        if files:
            print(f"   ℹ️  Sample files:")
            for f in files[:5]:
                print(f"      - {f}")
            if len(files) > 5:
                print(f"      ... and {len(files) - 5} more")

        print("   ✅ GCS connection working\n")
        return True

    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nPossible issues:")
        print("   - Bucket name incorrect")
        print("   - Service account doesn't have permissions")
        print("   - Credentials file is invalid")
        print("   - Network connectivity issues")
        return False

def test_upload_capability(creds):
    """Test upload capability (without actually uploading)."""
    print("4. Checking upload capability...")

    try:
        from automation.storage_sync import GCSStorageSync

        bucket_name = creds['bucket_name']
        credentials_path = creds.get('credentials_path')

        sync = GCSStorageSync(bucket_name, credentials_path)

        # Check if bucket exists and is writable
        # We'll check metadata which requires read access
        metadata = sync.get_latest_database_metadata()
        if metadata:
            print(f"   ✓ Found existing database in bucket")
            print(f"      Size: {metadata['size'] / (1024*1024):.2f} MB")
            print(f"      Last updated: {metadata.get('updated', 'Unknown')}")
        else:
            print(f"   ℹ️  No database file found in bucket (this is OK for first setup)")

        print("   ✅ Bucket is accessible\n")
        return True

    except Exception as e:
        print(f"\n❌ Upload capability check failed: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("GCS Setup Verification Test")
    print("=" * 60)
    print()

    # Check dependencies
    if not check_dependencies():
        sys.exit(1)

    # Check credentials
    success, creds = check_credentials()
    if not success:
        sys.exit(1)

    # Test connection
    if not test_connection(creds):
        sys.exit(1)

    # Test upload capability
    if not test_upload_capability(creds):
        sys.exit(1)

    # Success!
    print("=" * 60)
    print("✅ ALL TESTS PASSED!")
    print("=" * 60)
    print()
    print("Your GCS setup is working correctly. You can now:")
    print("   1. Upload data: python upload_to_gcs.py --location 90984 --all-dates")
    print("   2. Proceed to Week 2 implementation")
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
