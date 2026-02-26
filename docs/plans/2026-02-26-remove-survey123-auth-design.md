# Remove Survey123 Auth Route — Design Document

**Date:** 2026-02-26
**Status:** Approved
**Goal:** Eliminate the unused Survey123 OAuth2 sync pathway, making the public FeatureServer the sole cloud sync method.

## Context

The Cloud Function supports two sync modes: `survey123` (authenticated OAuth2) and `feature_server` (public REST API). The Survey123 path was implemented before discovering the public FeatureServer endpoint. Credentials were never obtained, and the FeatureServer mode is the only path running in production. The two paths access the same underlying Blue Thumb data — the FeatureServer is simply the public view.

## Scope Decisions

- **GCP resource names** (function name `survey123-daily-sync`, Cloud Scheduler job) stay as-is. Renaming is a separate infra step.
- **Directory name** `cloud_functions/survey123_sync/` stays as-is. Rename tracked as a future cleanup.
- **Mode routing** is removed entirely (YAGNI — one sync source, no need for mode selection).
- **`DatabaseManager` metadata methods** lose their default `metadata_blob_name` parameters. Callers must pass the blob name explicitly.

## What Gets Removed

### Source Code (`cloud_functions/survey123_sync/main.py`)
- `ArcGISAuthenticator` class (OAuth2 token management)
- `Survey123DataFetcher` class (private API fetcher)
- `process_survey123_data` function
- `_get_sync_mode` function (mode routing)
- Environment variable constants: `ARCGIS_CLIENT_ID`, `ARCGIS_CLIENT_SECRET`, `SURVEY123_FORM_ID`
- ArcGIS endpoint constants: `ARCGIS_TOKEN_URL`, `SURVEY123_API_BASE`
- Entire Survey123 branch in `survey123_daily_sync` (lines 457-543)

### Source Code (`cloud_functions/survey123_sync/chemical_processor.py`)
- `process_survey123_chemical_data` function

### Test Files (deleted entirely)
- `tests/survey123_sync/test_arcgis_auth.py`
- `tests/survey123_sync/test_survey123_fetcher.py`

### Test Files (modified — remove Survey123-specific tests)
- `tests/survey123_sync/test_data_processing.py` — remove `_get_sync_mode` and `process_survey123_data` tests
- `tests/survey123_sync/test_chemical_processor.py` — remove `process_survey123_chemical_data` tests

## What Gets Modified

### `main.py` Entry Point
- `survey123_daily_sync` becomes a thin wrapper: create `DatabaseManager`, call `_run_feature_server_sync`, handle errors
- Entry point name stays `survey123_daily_sync` (GCP config dependency) with a comment noting the legacy name
- No mode detection — goes straight to FeatureServer sync

### `DatabaseManager` Class
- `get_last_sync_timestamp` — `metadata_blob_name` becomes a required parameter (no default)
- `update_sync_timestamp` — same, `metadata_blob_name` becomes required
- `download_database` and `upload_database` — unchanged

### `_run_feature_server_sync`
- No logic changes. Already passes blob name explicitly.

### `deploy.sh`
- Remove "Next steps" echo block about Survey123 credentials
- Remove `SYNC_MODE` references
- Add TODO comment about renaming function and directory

### Documentation
- `docs/cloud/DEPLOYMENT.md` — remove dual-mode docs, Survey123 env vars, Survey123 sync flow
- `cloud_functions/survey123_sync/README.md` — rewrite for single FeatureServer path
- `CLAUDE.md` — update references to dual sync modes
- Root `README.md` — replace "Dual Sync Modes" with FeatureServer-only description
- `data_processing/arcgis_sync.py` — update comment referencing `process_survey123_chemical_data()`

## What Does NOT Change

- `data_processing/arcgis_sync.py` — no logic changes (comment update only)
- `chemical_processor.py` — only `process_survey123_chemical_data` deleted; `get_reference_values_from_db`, `insert_processed_data_to_db`, `classify_active_sites_in_db` untouched
- `_run_feature_server_sync` — no logic changes
- `DatabaseManager.download_database` / `upload_database` — untouched
- `database/database.py` — untouched (GCS daemon is unrelated)

## Verification Strategy

1. Run full test suite before changes to establish green baseline
2. Make changes
3. Run full test suite after — all remaining tests pass
4. Verify `_run_feature_server_sync` behavior is unchanged (no logic edits)
5. Verify `insert_processed_data_to_db` and `classify_active_sites_in_db` retain test coverage
6. Verify `DatabaseManager` download/upload tests still pass

## Risk Assessment

**Low risk.** We are removing dead code, not changing live behavior. The FeatureServer path is already running in production. The only functional change is that the function no longer accepts `?mode=survey123`, which was never used.

## Post-Merge Steps

1. **Redeploy Cloud Function** via `deploy.sh` (auto-deploy only covers Cloud Run, not Cloud Functions)
2. **Future cleanup:** Rename `cloud_functions/survey123_sync/` directory
3. **Future cleanup:** Rename GCP function name and Cloud Scheduler job in console
4. **Future cleanup:** Delete `sync_metadata/last_sync.json` blob from GCS bucket (Survey123 metadata, now orphaned)
