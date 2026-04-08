#!/usr/bin/env python3
"""
Setup verification script for DoughZone Dashboard.
Checks that all dependencies, files, and data structure are in place for BigQuery.
"""

import sys
import os
from pathlib import Path
import subprocess

class SetupChecker:
    def __init__(self):
        self.checks_passed = 0
        self.checks_failed = 0
        self.base_dir = Path(__file__).parent.parent

    def print_header(self, text):
        print(f"\n{'='*60}")
        print(f"  {text}")
        print(f"{'='*60}")

    def print_check(self, name, passed, message=""):
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status:10} | {name}")
        if message:
            print(f"           | {message}")
        if passed:
            self.checks_passed += 1
        else:
            self.checks_failed += 1

    def check_python_version(self):
        """Check Python version is 3.8+"""
        version = sys.version_info
        passed = version.major >= 3 and version.minor >= 8
        self.print_check(
            "Python version",
            passed,
            f"Found Python {version.major}.{version.minor}.{version.micro}"
        )
        return passed

    def check_required_files(self):
        """Check that all required Python files exist"""
        required_files = [
            "database/bigquery.py",
            "database/import_data.py",
            "app.py",
            "requirements.txt"
        ]
        all_exist = True
        for file in required_files:
            path = self.base_dir / file
            exists = path.exists()
            self.print_check(
                f"File: {file}",
                exists,
                f"Path: {path}"
            )
            all_exist = all_exist and exists
        return all_exist

    def check_dependencies(self):
        """Check that required packages are installed"""
        required_packages = [
            ("streamlit", "Streamlit"),
            ("pandas", "Pandas"),
            ("plotly", "Plotly"),
            ("google.cloud.bigquery", "Google BigQuery"),
            ("watchdog", "Watchdog"),
            ("openpyxl", "OpenPyXL")
        ]

        all_installed = True
        for package, display_name in required_packages:
            try:
                __import__(package)
                self.print_check(f"Package: {display_name}", True)
            except ImportError:
                self.print_check(
                    f"Package: {display_name}",
                    False,
                    f"Install with: pip install requirements.txt"
                )
                all_installed = False

        return all_installed

    def check_credentials(self):
        """Check for GCS/BQ credentials"""
        env_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        # Check if .env exists
        env_exits = (self.base_dir / ".env").exists()

        passed = False
        msg = "No credentials found"

        if env_creds and Path(env_creds).exists():
             passed = True
             msg = f"Found env var pointing to {env_creds}"
        elif (self.base_dir / "doughzone-gcs-key.json").exists():
             passed = True
             msg = "Found doughzone-gcs-key.json in root"

        self.print_check("GCP Credentials", passed, msg)
        return passed

    def check_toast_credentials(self):
        """Check for Toast API credentials and optionally test live auth."""
        import json

        creds_path = Path(os.getenv("TOAST_CREDENTIALS_PATH", self.base_dir / "toast_credentials.json"))

        # 1. File exists?
        if not creds_path.exists():
            self.print_check(
                "Toast credentials file",
                False,
                f"Not found at {creds_path}. Create it with keys: apiHostname, clientId, clientSecret, userAccessType"
            )
            return False

        # 2. Required keys present?
        required_keys = {"apiHostname", "clientId", "clientSecret", "userAccessType"}
        try:
            with open(creds_path) as f:
                creds = json.load(f)
        except Exception as e:
            self.print_check("Toast credentials file", False, f"Could not parse JSON: {e}")
            return False

        missing = required_keys - set(creds.keys())
        if missing:
            self.print_check(
                "Toast credentials keys",
                False,
                f"Missing key(s): {', '.join(sorted(missing))}"
            )
            return False

        self.print_check("Toast credentials file", True, f"Found at {creds_path} with all required keys")

        # 3. Live auth test
        try:
            import requests
            resp = requests.post(
                f"{creds['apiHostname'].rstrip('/')}/authentication/v1/authentication/login",
                json={
                    "clientId": creds["clientId"],
                    "clientSecret": creds["clientSecret"],
                    "userAccessType": creds["userAccessType"],
                },
                timeout=10,
            )
            resp.raise_for_status()
            token = resp.json().get("token", {}).get("accessToken")
            if token:
                self.print_check("Toast API live auth", True, "Token obtained successfully (24hr expiry, auto-refreshed)")
                return True
            else:
                self.print_check("Toast API live auth", False, "Auth response missing token — check clientId/clientSecret")
                return False
        except Exception as e:
            self.print_check("Toast API live auth", False, f"{type(e).__name__}: {e}")
            return False

    def run_all_checks(self):
        """Run all setup checks"""
        self.print_header("DOUGHZONE DASHBOARD - SETUP CHECK")

        print("\n1. PYTHON ENVIRONMENT")
        self.check_python_version()

        print("\n2. REQUIRED FILES")
        self.check_required_files()

        print("\n3. DEPENDENCIES")
        self.check_dependencies()

        print("\n4. CREDENTIALS")
        self.check_credentials()

        print("\n5. TOAST API")
        print("           | Toast uses a 24-hour access token obtained automatically from your")
        print("           | clientId and clientSecret — no manual token management needed.")
        print("           | Credentials live in toast_credentials.json (path set via")
        print("           | TOAST_CREDENTIALS_PATH in .env). If auth fails, contact the")
        print("           | Toast API portal to regenerate clientId/clientSecret.")
        self.check_toast_credentials()

        # Summary
        self.print_header("SUMMARY")
        total = self.checks_passed + self.checks_failed
        print(f"Checks passed: {self.checks_passed}/{total}")
        print(f"Checks failed: {self.checks_failed}/{total}")

        if self.checks_failed == 0:
            print("\n✓ All checks passed! You're ready to go.")
            return 0
        else:
            print(f"\n✗ {self.checks_failed} check(s) failed.")
            return 1


def main():
    checker = SetupChecker()
    exit_code = checker.run_all_checks()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
