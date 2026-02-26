# Deployment & Cloud Infrastructure

## Environment Detection

`config/gcp_config.py` determines the environment based on environment variables:

```python
# GCP if any of these are set:
GOOGLE_CLOUD_PROJECT    # GCP project ID
GAE_APPLICATION         # App Engine
K_SERVICE               # Cloud Run service name
```

| Setting | Local | GCP |
|---------|-------|-----|
| Database path | `database/blue_thumb.db` | `/tmp/blue_thumb.db` (ephemeral) |
| Asset base URL | `/assets` | `https://storage.googleapis.com/{GCS_ASSET_BUCKET}` |
| Log level | DEBUG | INFO |
| Debug mode | True | False |

## Local Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m database.reset_database    # Load data into SQLite
python app.py                        # Starts on http://127.0.0.1:8050
```

## Cloud Run Deployment

### Docker

`Dockerfile` builds on `python:3.12-slim`:
- Installs dependencies from `requirements.txt`
- Creates log directory structure
- Runs via Gunicorn on port 8080 (1 worker, 8 threads)

### Continuous Deployment (GitHub → Cloud Run)

Pushes to `main` on GitHub automatically build and deploy via Cloud Build:

1. Cloud Build trigger detects push to `main`
2. Builds Docker image from `Dockerfile`
3. Deploys new revision to Cloud Run (us-central1, allow-unauthenticated)

The trigger was configured through the Cloud Run console ("Set up continuous deployment" → Cloud Build → GitHub).


### Required Environment Variables (Cloud Run)

```
GOOGLE_CLOUD_PROJECT=blue-thumb-dashboard
GCS_BUCKET_DATABASE=blue-thumb-database
GCS_ASSET_BUCKET=blue-thumb-assets
```

Optional:
```
GCS_DB_BLOB_NAME=blue_thumb.db          # Blob name in bucket (default: blue_thumb.db)
DB_REFRESH_INTERVAL_SECONDS=300         # How often to check GCS for DB updates (default: 300)
```

Vertex AI (chatbot) authenticates via the Cloud Run service account — no API key needed.

### Database Refresh on Cloud Run

Cloud Run no longer relies solely on the Docker-baked database. On startup, `database.py` downloads the latest database from GCS and starts a background daemon thread that polls for updates by comparing the GCS blob generation number. Each incoming request also triggers a lightweight generation check (rate-limited). This keeps the dashboard in sync with Cloud Function updates without requiring redeployment.

## Cloud Function: Data Sync

Located in `cloud_functions/survey123_sync/`. Fetches new chemical data from the public ArcGIS FeatureServer and updates the database.

> **Note:** The `survey123_sync` directory and function entry point names are legacy — retained for GCP config compatibility. TODO: Rename to `data_sync`.

### Components
- **`main.py`** — Entry point. Calls FeatureServer sync directly
- **`chemical_processor.py`** — Chemical value processing, status classification, site reclassification, and idempotent DB insertion with `sample_id` support

### Sync Strategy

The sync uses an adaptive strategy:

1. **First run** (no prior FeatureServer sync metadata): Fetches by sampling date (`day` field) from the DB's latest chemical date
2. **Subsequent runs**: Fetches by `EditDate` timestamp from the last successful sync, catching both new records and edits to existing ones

Sync metadata is stored separately at `sync_metadata/last_feature_server_sync.json` in GCS.

### Deployment

```bash
cd cloud_functions/survey123_sync
./deploy.sh
```

Deploy config: Python 3.12, 512MB memory, 540s timeout, us-central1, max 1 instance.

`deploy.sh` creates a staging directory that bundles the function code with shared project modules (`utils.py`, `config/`, `data_processing/`, `database/`). The bundled database file is excluded from staging.

### Required Environment Variables (Cloud Function)

```
GCS_BUCKET_DATABASE=blue-thumb-database
```

Optional:
```
GCS_DB_BLOB_NAME=blue_thumb.db  # Blob name in bucket (default: blue_thumb.db)
```

### Cloud Scheduler

Triggers the Cloud Function daily at 6 AM Central:

```bash
gcloud scheduler jobs create http survey123-daily-sync \
  --schedule="0 6 * * *" \
  --uri="<FUNCTION_URL>" \
  --http-method=POST \
  --time-zone="America/Chicago"
```

### Sync Flow

1. Download database from Cloud Storage to temp file
2. Determine sync strategy (date-based or EditDate-based)
3. Fetch QAQC-verified records from public FeatureServer
4. Translate FeatureServer field names to pipeline schema
5. Process through shared chemical pipeline
6. Insert with `sample_id`-based idempotency (no duplicates on re-sync)
7. Reclassify sites as active/historic
8. Upload updated database back to Cloud Storage
9. Record sync metadata (strategy, marker, record counts)
10. Clean up temp DB file

## Logging

Organized by category in `logs/`:

```
logs/
├── app/            → Application startup
├── callbacks/      → Callback execution
├── database/       → DB operations
├── general/        → Config, utilities
├── preprocessing/  → Data cleaning
├── processing/     → ETL pipeline
├── testing/        → Test execution
├── utils/          → Utility functions
└── visualization/  → Chart generation
```

Setup via `utils.setup_logging(module_name, category=...)`.

In cloud environments (Cloud Run, Cloud Functions), `setup_logging()` detects the environment via `K_SERVICE`, `K_REVISION`, `FUNCTION_TARGET`, or `GAE_APPLICATION` and writes logs to `/tmp` instead of searching for the project root.
