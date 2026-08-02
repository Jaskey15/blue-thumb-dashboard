---
description: Cloud deployment — Cloud Run, Cloud Functions, environment detection, sync strategy
globs:
  - cloud_functions/**
  - config/**
  - Dockerfile
  - deploy.sh
  - app.py
---

# Cloud Infrastructure

## Environment Detection (`config/gcp_config.py`)

GCP detected via: `GOOGLE_CLOUD_PROJECT`, `GAE_APPLICATION`, or `K_SERVICE` env vars.

| Setting | Local | GCP |
|---------|-------|-----|
| DB path | `database/blue_thumb.db` | `/tmp/blue_thumb.db` |
| Log level | DEBUG | INFO |
| Debug mode | True | False |

## Cloud Run

- Docker image: `python:3.12-slim`, Gunicorn on port 8080
- CD: push to `main` → Cloud Build trigger → Docker build → Cloud Run deploy (us-central1)
- Vertex AI chatbot authenticates via service account, no API key needed
- Required env vars: `GOOGLE_CLOUD_PROJECT`, `GCS_BUCKET_DATABASE`

### Scaling and cost

- **`min-instances: 0`** (scale to zero). An always-on instance costs ~$15/month; request-based
  billing charges only for request handling plus startup, so idle instances are free.
- Cold start is ~5s, almost entirely Dash/pandas/Plotly import time. The GCS database
  download is not a factor (5.6 MB, fetched lazily on first `get_connection()`).
- `/health` in `app.py` is a keepalive target — no DB access, no layout rendering. Cloud
  Scheduler job `blue-thumb-keepalive` pings it `*/10 6-22` Central to keep one instance
  warm. Idle retention (~15 min) is observed behavior, not a documented SLA, so this
  reduces cold starts but cannot eliminate them.

### Artifact Registry cleanup

Never apply an **age-based** cleanup policy to `cloud-run-source-deploy`. Deploys are
infrequent, so the currently-serving image ages past any "older than N days" threshold
while still in use. Deleting it fails silently — the running instance serves from local
cache, then the next cold start breaks hours later with no obvious cause. Use count-based
(`keep N most recent`) policies only, and always `--dry-run` first.

## Cloud Function (Data Sync)

Located in `cloud_functions/data_sync/`.

**Sync strategy:**
1. First run (no prior sync metadata): fetch by sampling date from DB's latest chemical date
2. Subsequent runs: fetch by `EditDate` from last successful sync timestamp
3. Metadata stored at `sync_metadata/last_feature_server_sync.json` in GCS

**Deploy:** `cd cloud_functions/data_sync && ./deploy.sh`
- `deploy.sh` creates staging dir bundling function code + shared project modules
- Cloud Scheduler triggers daily at 6 AM Central

## Logging

`utils.setup_logging(module_name, category=...)` — in cloud envs, writes to `/tmp` instead of project root.
