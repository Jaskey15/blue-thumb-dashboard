---
description: Cloud deployment — Cloud Run, Cloud Functions, environment detection, sync strategy
globs:
  - cloud_functions/**
  - config/**
  - Dockerfile
  - deploy.sh
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
