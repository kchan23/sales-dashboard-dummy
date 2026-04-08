#!/bin/bash
# Helper script to set up .env file with GCS credentials
# Run this after you've completed GCP setup and downloaded your service account key

echo "=================================="
echo "GCS Environment Setup Helper"
echo "=================================="
echo ""

# Check if .env already exists
if [ -f .env ]; then
    echo "⚠️  .env file already exists!"
    read -p "Do you want to overwrite it? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Setup cancelled."
        exit 0
    fi
fi

# Prompt for GCP Project ID
echo "Step 1: Enter your GCP Project ID"
echo "   (Find this in GCP Console → Project Info)"
read -p "Project ID: " PROJECT_ID

# Prompt for Bucket Name
echo ""
echo "Step 2: Enter your GCS Bucket Name"
echo "   (Default: doughzone-data)"
read -p "Bucket Name [doughzone-data]: " BUCKET_NAME
BUCKET_NAME=${BUCKET_NAME:-doughzone-data}

# Prompt for Service Account Key Path
echo ""
echo "Step 3: Enter the path to your service account JSON key file"
echo "   (The file you downloaded from GCP Console)"
read -p "Key file path [./doughzone-gcs-key.json]: " KEY_PATH
KEY_PATH=${KEY_PATH:-./doughzone-gcs-key.json}

# Check if key file exists
if [ ! -f "$KEY_PATH" ]; then
    echo ""
    echo "⚠️  Warning: Key file not found at: $KEY_PATH"
    echo "   Make sure to download and place your service account key file there!"
fi

# Create .env file
cat > .env << EOF
# Google Cloud Storage Configuration
# Generated on $(date)

# GCS Project ID
GCS_PROJECT_ID=$PROJECT_ID

# GCS Bucket Name
GCS_BUCKET_NAME=$BUCKET_NAME

# Path to service account JSON key file
GOOGLE_APPLICATION_CREDENTIALS=$KEY_PATH

# Optional: Logging level
LOG_LEVEL=INFO
EOF

echo ""
echo "✅ .env file created successfully!"
echo ""
echo "Configuration:"
echo "  Project ID: $PROJECT_ID"
echo "  Bucket Name: $BUCKET_NAME"
echo "  Key File: $KEY_PATH"
echo ""
echo "Next steps:"
echo "  1. Verify your service account key is at: $KEY_PATH"
echo "  2. Test the setup: python test_gcs_setup.py"
echo "  3. Upload data: python upload_to_gcs.py --location 90984 --all-dates"
echo ""
