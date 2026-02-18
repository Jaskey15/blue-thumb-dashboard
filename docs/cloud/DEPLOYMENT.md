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

`Dockerfile` builds on `python:3.9-slim`:
- Installs dependencies from `requirements.txt`
- Creates log directory structure
- Runs via Gunicorn on port 8080 (1 worker, 8 threads)

### Cloud Build CI/CD

`cloudbuild.yaml` pipeline:
1. **Build** → `gcr.io/$PROJECT_ID/blue-thumb-dashboard:$BUILD_ID`
2. **Push** → Google Container Registry
3. **Deploy** → Cloud Run (us-central1, allow-unauthenticated)

Build machine: E2_HIGHCPU_8.

### Required Environment Variables (Cloud Run)

```
GOOGLE_CLOUD_PROJECT=<project-id>
GCS_ASSET_BUCKET=blue-thumb-assets
GOOGLE_GENAI_API_KEY=<vertex-ai-key>
```

## Cloud Function: Survey123 Sync

Located in `cloud_functions/survey123_sync/`. Runs daily to fetch new Survey123 submissions and update the database.

### Components
- **`main.py`** — Entry point. ArcGIS OAuth2 auth → fetch submissions → process → upload DB
- **`chemical_processor.py`** — Handles range-based chemical value processing and status classification

### Deployment

```bash
cd cloud_functions/survey123_sync
./deploy.sh
```

Deploy config: Python 3.12, 512MB memory, 540s timeout, us-central1.

### Required Environment Variables (Cloud Function)

```
GCS_BUCKET_DATABASE=blue-thumb-database
ARCGIS_CLIENT_ID=<service-account-id>
ARCGIS_CLIENT_SECRET=<service-account-secret>
SURVEY123_FORM_ID=<form-id>
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

1. Download database from Cloud Storage bucket
2. Create backup of current database
3. Authenticate with ArcGIS (OAuth2 with token refresh)
4. Fetch new Survey123 submissions since last sync
5. Process chemical data (range selection, status classification)
6. Insert into local SQLite
7. Upload updated database back to Cloud Storage

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
