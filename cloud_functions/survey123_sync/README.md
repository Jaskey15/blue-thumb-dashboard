# FeatureServer Daily Sync Cloud Function

> **Note:** The `survey123_sync` directory and entry point name are legacy — retained for GCP config compatibility. TODO: Rename to `data_sync`.

This Cloud Function provides automated daily synchronization of chemical data from the public ArcGIS FeatureServer with the Blue Thumb Dashboard database.

## Overview

The sync function:
- **Runs daily at 6 AM Central Time** via Cloud Scheduler
- **Fetches new records** from the public ArcGIS FeatureServer (no auth required)
- **Processes chemical data** using existing Blue Thumb logic
- **Updates SQLite database** stored in Cloud Storage
- **Creates automatic backups** before each update
- **Provides comprehensive logging** for monitoring and debugging

## Architecture

```
ArcGIS FeatureServer ➜ Cloud Function ➜ Cloud Storage ➜ Dashboard
                            ↓
                     Processing Logic
                   (Chemical Analysis)
```

## Files

- **`main.py`**: Cloud Function entry point and orchestration
- **`chemical_processor.py`**: Chemical data processing, status classification, and idempotent DB insertion
- **`requirements.txt`**: Python dependencies
- **`deploy.sh`**: Automated deployment script
- **`README.md`**: This documentation

## Setup Instructions

### 1. Deploy Cloud Function

```bash
cd cloud_functions/survey123_sync
chmod +x deploy.sh
./deploy.sh
```

### 2. Create Daily Schedule

Create a Cloud Scheduler job for daily execution:

```bash
gcloud scheduler jobs create http survey123-daily-sync \
    --schedule="0 6 * * *" \
    --uri="https://us-central1-blue-thumb-dashboard.cloudfunctions.net/survey123-daily-sync" \
    --http-method=POST \
    --time-zone="America/Chicago"
```

### 3. Upload Database

Ensure your SQLite database is uploaded to Cloud Storage:

```bash
gsutil cp database/blue_thumb.db gs://blue-thumb-database/
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GOOGLE_CLOUD_PROJECT` | GCP project ID | Yes (auto-set) |
| `GCS_BUCKET_DATABASE` | Cloud Storage bucket for database | Yes |
| `GCS_DB_BLOB_NAME` | Blob name in bucket (default: `blue_thumb.db`) | No |

## Data Processing Flow

### 1. Sync Strategy

The function uses an adaptive sync strategy:

1. **First run** (no prior sync metadata): Fetches by sampling date (`day` field) from the DB's latest chemical date
2. **Subsequent runs**: Fetches by `EditDate` timestamp from the last successful sync, catching both new records and edits to existing ones

Sync metadata is stored at `sync_metadata/last_feature_server_sync.json` in GCS.

### 2. Chemical Processing
- **Nutrient processing**: Handles range-based measurements (Low/Mid/High)
- **BDL conversions**: Converts zeros to Below Detection Limit values
- **Data validation**: Removes invalid measurements
- **Schema formatting**: Converts to database-compatible format

### 3. Database Updates
- Downloads SQLite database from Cloud Storage
- Creates automatic backup with timestamp
- Inserts new chemical measurements with `sample_id`-based idempotency (no duplicates on re-sync)
- Reclassifies active/historic site status based on updated data
- Uploads updated database back to Cloud Storage

### 4. Sync Tracking
- Records last successful sync timestamp and metadata
- Provides detailed execution logs
- Returns comprehensive status information

## Chemical Parameters Processed

| Parameter | Processing Logic |
|-----------|-----------------|
| **pH** | Greater of two readings |
| **Dissolved Oxygen** | Direct mapping (% saturation) |
| **Nitrate** | Greater of two readings |
| **Nitrite** | Greater of two readings |
| **Ammonia** | Conditional based on range selection (Low/Mid/High) |
| **Phosphorus** | Conditional based on range selection (Low/Mid/High) |
| **Chloride** | Conditional based on range selection (Low/Mid/High) |

### Range-Based Processing

For Ammonia, Phosphorus, and Chloride, the function uses conditional logic:

```python
# Example for Ammonia
if range_selection == "Low":
    value = max(low_reading_1, low_reading_2)
elif range_selection == "Mid":
    value = max(mid_reading_1, mid_reading_2)
elif range_selection == "High":
    value = max(high_reading_1, high_reading_2)
```

### Active/Historic Site Classification

After inserting new chemical data, the function automatically reclassifies all sites:

- **Active Sites**: Have chemical readings within 1 year of the most recent reading date across all sites
- **Historic Sites**: No recent chemical data or readings older than 1-year cutoff
- **Process**: Updates the `active` flag and `last_chemical_reading_date` for all sites in the database

## Monitoring and Debugging

### View Function Logs

```bash
gcloud functions logs read survey123-daily-sync --region=us-central1
```

### Test Manual Execution

```bash
# Get function URL
FUNCTION_URL=$(gcloud functions describe survey123-daily-sync --region=us-central1 --format="value(serviceConfig.uri)")

# Trigger manually
curl -X POST $FUNCTION_URL
```

### Check Scheduler Status

```bash
gcloud scheduler jobs describe survey123-daily-sync --location=us-central1
```

## Response Format

### Successful Execution
```json
{
  "status": "success",
  "mode": "feature_server",
  "message": "Successfully processed 3 new records",
  "records_fetched": 3,
  "records_processed": 3,
  "records_inserted": 21,
  "execution_time": "0:00:45.123456",
  "sync_strategy": "editdate",
  "site_classification": {
    "sites_classified": 370,
    "active_count": 85,
    "historic_count": 285
  }
}
```

### No New Data
```json
{
  "status": "success",
  "message": "No new data to process",
  "records_fetched": 0,
  "records_processed": 0,
  "execution_time": "0:00:12.345678"
}
```

### Error Response
```json
{
  "status": "failed",
  "error": "Sync failed: Failed to download database",
  "execution_time": "0:00:05.123456"
}
```

## Cost Estimation

**Daily execution costs** (very minimal):
- **Function invocations**: ~$0.0001/year (within free tier)
- **Compute time**: ~$0.30/year
- **Storage**: ~$0.36/year (database storage)
- **Total**: **<$1/year**

## Troubleshooting

### Common Issues

1. **Database Update Failed**
   - Check Cloud Storage bucket permissions
   - Verify database file exists in bucket

2. **No Data Processed**
   - Check if there are new FeatureServer records since last sync
   - Verify sync metadata timestamp in GCS

3. **Function Timeout**
   - Large backfills may exceed the 540s timeout
   - Consider running with a narrower date range

## Security Considerations

- **No credentials required** — FeatureServer is a public endpoint
- **Database backups** are created before each update
- **Function access** can be restricted using IAM policies
