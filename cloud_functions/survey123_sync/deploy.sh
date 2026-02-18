#!/bin/bash

# Deploy Survey123 Sync Cloud Function
# This script deploys the daily sync function to Google Cloud

set -e

# Configuration
FUNCTION_NAME="survey123-daily-sync"
REGION="us-central1"
RUNTIME="python312"
MEMORY="512MB"
TIMEOUT="540s"
MAX_INSTANCES="1"

# Environment variables (these should be set via Secret Manager in production)
ENV_VARS="GCS_BUCKET_DATABASE=blue-thumb-database"

echo "Deploying Survey123 Daily Sync Cloud Function..."
echo "Function: $FUNCTION_NAME"
echo "Region: $REGION"
echo "Runtime: $RUNTIME"

# Change to the function's directory to correctly set the source
cd "$(dirname "$0")"

# Deploy the function
gcloud functions deploy $FUNCTION_NAME \
    --gen2 \
    --runtime=$RUNTIME \
    --region=$REGION \
    --source=. \
    --entry-point=survey123_daily_sync \
    --trigger-http \
    --memory=$MEMORY \
    --timeout=$TIMEOUT \
    --max-instances=$MAX_INSTANCES \
    --set-env-vars=$ENV_VARS \
    --allow-unauthenticated

echo "Function deployed successfully!"

# Get the function URL
FUNCTION_URL=$(gcloud functions describe $FUNCTION_NAME --region=$REGION --format="value(serviceConfig.uri)")
echo "Function URL: $FUNCTION_URL"

echo ""
echo "Next steps:"
echo "1. Set up environment variables for ArcGIS credentials:"
echo "   - ARCGIS_CLIENT_ID"
echo "   - ARCGIS_CLIENT_SECRET" 
echo "   - SURVEY123_FORM_ID"
echo ""
echo "2. Create daily Cloud Scheduler job:"
echo "   gcloud scheduler jobs create http survey123-daily-sync \\"
echo "     --schedule=\"0 6 * * *\" \\"
echo "     --uri=\"$FUNCTION_URL\" \\"
echo "     --http-method=POST \\"
echo "     --time-zone=\"America/Chicago\""
echo ""
echo "3. Upload your SQLite database to Cloud Storage bucket: blue-thumb-database" 