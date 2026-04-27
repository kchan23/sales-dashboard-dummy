# Google Cloud Storage Setup Guide

This guide walks you through setting up Google Cloud Storage for the restaurant analytics project.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Step 1: Create Google Cloud Project](#step-1-create-google-cloud-project)
- [Step 2: Enable Cloud Storage API](#step-2-enable-cloud-storage-api)
- [Step 3: Create Storage Bucket](#step-3-create-storage-bucket)
- [Step 4: Create Folder Structure](#step-4-create-folder-structure)
- [Step 5: Create Service Account](#step-5-create-service-account)
- [Step 6: Generate Service Account Key](#step-6-generate-service-account-key)
- [Step 7: Configure Local Environment](#step-7-configure-local-environment)
- [Step 8: Install Dependencies](#step-8-install-dependencies)
- [Step 9: Test Connection](#step-9-test-connection)
- [Step 10: Upload Existing Data](#step-10-upload-existing-data)
- [Setting up Streamlit Cloud Secrets](#setting-up-streamlit-cloud-secrets)
- [Billing Alerts](#billing-alerts-recommended)
- [Cost Estimate](#cost-estimate)
- [Troubleshooting](#troubleshooting)
- [Security Best Practices](#security-best-practices)
- [Next Steps](#next-steps)

## Prerequisites

- Google Cloud Platform account (free tier works fine)
- Credit card for GCP account verification (won't be charged for prototype usage)

## Step 1: Create Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click "Select a project" → "New Project"
3. Project name: `doughzone-analytics` (or your choice)
4. Click "Create"
5. Wait for project creation (takes ~30 seconds)

## Step 2: Enable Cloud Storage API

1. In the GCP Console, go to "APIs & Services" → "Library"
2. Search for "Cloud Storage API"
3. Click "Enable"

## Step 3: Create Storage Bucket

1. Go to "Cloud Storage" → "Buckets"
2. Click "Create Bucket"
3. Configuration:
   - **Name**: `doughzone-data` (must be globally unique, add suffix if taken)
   - **Location type**: Region
   - **Region**: `us-west1` (or closest to you)
   - **Storage class**: Standard
   - **Access control**: Uniform
   - **Protection tools**: None (for prototype)
4. Click "Create"

## Step 4: Create Folder Structure

After bucket creation, create these folders:
- `raw/` - For CSV/Excel source files
- `logs/` - For import logs

You can do this via the GCS web UI or it will be created automatically on first upload.

## Step 5: Create Service Account

1. Go to "IAM & Admin" → "Service Accounts"
2. Click "Create Service Account"
3. Configuration:
   - **Name**: `doughzone-storage-admin`
   - **Description**: "Service account for DoughZone data uploads"
4. Click "Create and Continue"
5. **Grant roles**:
   - Role: `Storage Admin` (full control over bucket)
6. Click "Continue" → "Done"

## Step 6: Generate Service Account Key

1. In Service Accounts list, click the email of the account you just created
2. Go to "Keys" tab
3. Click "Add Key" → "Create new key"
4. Choose "JSON" format
5. Click "Create"
6. **IMPORTANT**: Save the downloaded JSON file securely!
   - Rename it to something memorable: `doughzone-gcs-key.json`
   - Store in your dashboard-app directory (it's already in .gitignore)
   - Never commit this file to Git!

## Step 7: Configure Local Environment

### Option A: Using .env file (Recommended for local development)

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` and fill in your values:
   ```bash
   GCS_PROJECT_ID=your-project-id
   GCS_BUCKET_NAME=doughzone-data
   GOOGLE_APPLICATION_CREDENTIALS=./doughzone-gcs-key.json
   ```

### Option B: Using environment variables

```bash
export GCS_PROJECT_ID=your-project-id
export GCS_BUCKET_NAME=doughzone-data
export GOOGLE_APPLICATION_CREDENTIALS=/full/path/to/doughzone-gcs-key.json
```

### Option C: Using gcloud CLI (Alternative)

```bash
gcloud auth application-default login
export GCS_BUCKET_NAME=doughzone-data
```

## Step 8: Install Dependencies

```bash
cd /home/kchan23/cpp/capstone/dashboard-app
pip install -r requirements.txt
```

This will install:
- `google-cloud-storage` - GCS Python client
- `python-dotenv` - Environment variable management
- `tqdm` - Progress bars

## Step 9: Test Connection

Test that everything works:

```bash
python -c "from automation.storage_sync import GCSStorageSync, load_credentials_from_env; \
creds = load_credentials_from_env(); \
sync = GCSStorageSync(creds['bucket_name'], creds['credentials_path']); \
print('✅ Successfully connected to GCS!')"
```

If you see "Successfully connected to GCS!", you're all set!

## Step 10: Upload Existing Data

Upload your current data to GCS:

```bash
# Upload single date
python upload_to_gcs.py --location 90984 --date 20250210

# Upload all dates for location 90984
python upload_to_gcs.py --location 90984 --all-dates
```

## Setting up Streamlit Cloud Secrets

When deploying to Streamlit Cloud, you'll need to add GCS credentials to Streamlit secrets:

1. Go to [Streamlit Cloud](https://share.streamlit.io/)
2. Open your app settings
3. Go to "Secrets" section
4. Add this configuration (replace with your values):

```toml
[gcs]
project_id = "your-project-id"
bucket_name = "doughzone-data"
credentials_json = '''
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "...",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "doughzone-storage-admin@your-project.iam.gserviceaccount.com",
  "client_id": "...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "..."
}
'''
```

To get the JSON content, open your service account key file and copy the entire contents.

## Billing Alerts (Recommended)

Set up billing alerts to avoid surprises:

1. Go to "Billing" in GCP Console
2. Select "Budgets & alerts"
3. Click "Create Budget"
4. Configuration:
   - **Name**: "DoughZone Storage Budget"
   - **Budget amount**: $20/month (way more than needed for prototype)
   - **Threshold**: Alert at 50%, 75%, 100%
   - **Email notifications**: Your email
5. Click "Finish"

## Cost Estimate

Based on your current data (1 location, 32 days):
- **Storage**: 25 GB × $0.020/GB = $0.50/month
- **Operations**: ~$0.02/month
- **Total**: ~$0.52/month

For 27 locations (full production):
- **Storage**: ~$7-14/month (depending on data retention)

## Troubleshooting

### Error: "Could not automatically determine credentials"
- Make sure `GOOGLE_APPLICATION_CREDENTIALS` points to correct JSON file
- Check that the file path is absolute, not relative
- Try: `export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/doughzone-gcs-key.json`

### Error: "Bucket not found"
- Verify bucket name in GCS Console
- Check for typos in bucket name
- Ensure bucket is in the correct project

### Error: "Permission denied"
- Verify service account has "Storage Admin" role
- Check that you're using the correct service account key
- Try deleting and regenerating the service account key

### Error: "Invalid service account JSON"
- Ensure JSON file is valid (not corrupted during download)
- Don't edit the JSON file manually
- Regenerate key if needed

## Security Best Practices

1. **Never commit service account keys to Git**
   - Already in `.gitignore` as `*-gcs-key.json` and `*.json`
   - Double-check before committing!

2. **Restrict service account permissions**
   - Only grant "Storage Admin" for the specific bucket
   - Don't use project-wide roles

3. **Rotate keys periodically**
   - Delete old keys when creating new ones
   - Update `.env` file with new key path

4. **Use Streamlit secrets for production**
   - Never put credentials in code
   - Use Streamlit Cloud secrets management

## Next Steps

Once GCS is set up and data is uploaded:
1. ✅ Week 1 Complete!
2. Continue to Week 2: Automated Import Pipeline
3. See main implementation plan for details

## Support

If you run into issues:
- Check [GCS Documentation](https://cloud.google.com/storage/docs)
- Review error messages carefully
- Ensure all prerequisites are met
- Double-check credentials and permissions
