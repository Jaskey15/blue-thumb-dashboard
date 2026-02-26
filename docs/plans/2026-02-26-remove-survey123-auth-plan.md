# Remove Survey123 Auth Route — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove the unused Survey123 OAuth2 sync pathway, making the public FeatureServer the sole cloud sync method.

**Architecture:** The Cloud Function entry point (`survey123_daily_sync`) will become a thin wrapper that creates a `DatabaseManager` and calls `_run_feature_server_sync` directly — no mode routing. The `DatabaseManager` metadata methods lose their default blob name parameters (callers pass explicitly). All Survey123-specific classes, functions, tests, and docs are removed.

**Tech Stack:** Python 3.12, Cloud Functions, Google Cloud Storage, pytest

**Baseline:** 44 tests passing in `tests/survey123_sync/` before changes. After changes, the Survey123-specific tests are removed and remaining tests still pass.

---

### Task 1: Delete Survey123-Only Test Files

These test files test exclusively dead code (ArcGISAuthenticator, Survey123DataFetcher). Delete them first so they don't fail when we remove the source code.

**Files:**
- Delete: `tests/survey123_sync/test_arcgis_auth.py`
- Delete: `tests/survey123_sync/test_survey123_fetcher.py`

**Step 1: Delete test files**

```bash
rm tests/survey123_sync/test_arcgis_auth.py
rm tests/survey123_sync/test_survey123_fetcher.py
```

**Step 2: Run remaining tests to verify nothing depended on them**

Run: `python -m pytest tests/survey123_sync/ -v --tb=short`
Expected: 30 tests pass (was 44; removed 14 from the two deleted files)

**Step 3: Commit**

```bash
git add -u tests/survey123_sync/
git commit -m "Remove Survey123 auth and fetcher test files

These tested ArcGISAuthenticator and Survey123DataFetcher which are
being removed as part of the Survey123 auth route elimination."
```

---

### Task 2: Remove Survey123-Specific Tests from Shared Test Files

Remove Survey123-specific test classes/methods from test files that also test FeatureServer functionality.

**Files:**
- Modify: `tests/survey123_sync/test_data_processing.py` — remove `TestSurvey123DataProcessing` class (lines 26-74), `test_get_sync_mode_precedence` method (lines 80-98), and the `process_survey123_data` import (line 23)
- Modify: `tests/survey123_sync/test_chemical_processor.py` — remove `process_survey123_chemical_data` import (line 29), remove three `test_process_survey123_*` methods (lines 124-184)

**Step 1: Update `test_data_processing.py`**

Remove the `process_survey123_data` import on line 23. Remove the entire `TestSurvey123DataProcessing` class (lines 26-74). Remove the `test_get_sync_mode_precedence` method from `TestSyncModeBehavior` (lines 80-98). Keep the `TestSyncModeBehavior` class with its two `test_run_feature_server_sync_*` methods. Also remove the now-unused `import main` reference to `_get_sync_mode` — but `main` is still needed for `_run_feature_server_sync`.

The file's imports should become:
```python
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

# ... path setup and mocks ...

import main
```

The file should contain only `TestSyncModeBehavior` with these two methods:
- `test_run_feature_server_sync_no_records_updates_metadata`
- `test_run_feature_server_sync_processes_and_uploads`

**Step 2: Update `test_chemical_processor.py`**

Remove `process_survey123_chemical_data` from the import on line 29. Remove these three test methods from `TestChemicalProcessor`:
- `test_process_survey123_chemical_data_success` (lines 124-162)
- `test_process_survey123_chemical_data_empty` (lines 164-171)
- `test_process_survey123_chemical_data_error_handling` (lines 173-184)

Keep all other test methods (reference values, insert, classify). The `sample_survey123_data` fixture in `setUp` can be removed too since nothing uses it after removing the survey123 tests. Rename `setUp` sample data to only keep `sample_reference_values` which is used by the remaining insert/classify tests.

**Step 3: Run tests to verify remaining tests pass**

Run: `python -m pytest tests/survey123_sync/ -v --tb=short`
Expected: 18 tests pass (removed `TestSurvey123DataProcessing` 2 tests, `test_get_sync_mode_precedence` 1 test, `process_survey123_chemical_data` 3 tests = 6 removed from 24 remaining = 18)

**Step 4: Commit**

```bash
git add tests/survey123_sync/test_data_processing.py tests/survey123_sync/test_chemical_processor.py
git commit -m "Remove Survey123-specific tests from shared test files

Remove process_survey123_data, _get_sync_mode, and
process_survey123_chemical_data test coverage. Retain all
FeatureServer sync and database operation tests."
```

---

### Task 3: Remove Survey123 Code from `main.py`

Remove all Survey123-specific code and mode routing from the Cloud Function entry point.

**Files:**
- Modify: `cloud_functions/survey123_sync/main.py`

**Step 1: Remove dead imports, constants, and classes**

Remove these sections:
- `ARCGIS_CLIENT_ID`, `ARCGIS_CLIENT_SECRET`, `SURVEY123_FORM_ID` env var constants (lines 45-47)
- `ARCGIS_TOKEN_URL`, `SURVEY123_API_BASE` constants (lines 50-51)
- `ArcGISAuthenticator` class (lines 53-88)
- `Survey123DataFetcher` class (lines 90-145)
- `_get_sync_mode` function (lines 233-252)
- `process_survey123_data` function (lines 417-432)

**Step 2: Make `DatabaseManager` metadata parameters required**

In `get_last_sync_timestamp` (line 191): change `metadata_blob_name: str = 'sync_metadata/last_sync.json'` to `metadata_blob_name: str` (no default).

In `update_sync_timestamp` (line 208): change `metadata_blob_name: str = 'sync_metadata/last_sync.json'` to `metadata_blob_name: str` (no default).

**Step 3: Simplify `survey123_daily_sync` entry point**

Replace the entire function body (lines 436-554) with a thin wrapper:

```python
@functions_framework.http
def survey123_daily_sync(request):
    """
    Cloud Function entry point for daily FeatureServer data sync.

    Fetches new chemical data from the public ArcGIS FeatureServer and
    updates the SQLite database in Cloud Storage.

    NOTE: Entry point name is legacy — retained for GCP config compatibility.
    TODO: Rename to 'data_sync' and update GCP function config.
    """
    start_time = datetime.now()
    logger.info(f"Starting FeatureServer data sync at {start_time}")

    try:
        db_manager = DatabaseManager(DATABASE_BUCKET)
        return _run_feature_server_sync(db_manager, start_time)

    except Exception as e:
        error_msg = f"Sync failed: {str(e)}"
        logger.error(error_msg)
        return {
            'status': 'failed',
            'error': error_msg,
            'execution_time': str(datetime.now() - start_time)
        }, 500
```

Also update the `if __name__ == "__main__"` block at the bottom to remove `MockRequest` (just pass `None` since mode routing is gone).

**Step 4: Clean up unused imports**

After removals, the top-level imports should be reviewed. Remove any imports only used by Survey123 code. The `requests` import may still be needed by other modules at deploy time but is not used directly in `main.py` after removal — check and remove if unused. Keep `functions_framework`, `json`, `logging`, `os`, `sqlite3`, `sys`, `tempfile`, `datetime`/`timedelta`/`timezone`, `Optional`, `pandas`, `storage`.

**Step 5: Run tests**

Run: `python -m pytest tests/survey123_sync/ -v --tb=short`
Expected: 18 tests pass. The two `_run_feature_server_sync` tests and all `DatabaseManager` tests should still pass.

**Step 6: Commit**

```bash
git add cloud_functions/survey123_sync/main.py
git commit -m "Remove Survey123 auth route from Cloud Function entry point

Remove ArcGISAuthenticator, Survey123DataFetcher, mode routing,
and Survey123-specific env vars. Entry point now calls
_run_feature_server_sync directly. DatabaseManager metadata
methods now require explicit blob name parameter."
```

---

### Task 4: Remove `process_survey123_chemical_data` from `chemical_processor.py`

**Files:**
- Modify: `cloud_functions/survey123_sync/chemical_processor.py`

**Step 1: Remove the function**

Delete `process_survey123_chemical_data` (lines 89-120). Keep `get_reference_values_from_db`, `insert_processed_data_to_db`, and `classify_active_sites_in_db`.

**Step 2: Clean up imports**

After removing `process_survey123_chemical_data`, check if any imports are now unused. The function uses `parse_sampling_dates`, `process_simple_nutrients`, `process_conditional_nutrient`, `format_to_database_schema`, `remove_empty_chemical_rows`, `validate_chemical_data`, `apply_bdl_conversions`. Of these, check which are still used by the remaining functions:
- `get_reference_values_from_db`: uses `pd.read_sql_query` only
- `insert_processed_data_to_db`: uses `determine_status`, `insert_collection_event`, `pd`, `sqlite3`
- `classify_active_sites_in_db`: uses `sqlite3`, `datetime`, `timedelta`

So these imports become unused and should be removed:
- `apply_bdl_conversions` from `chemical_utils`
- `remove_empty_chemical_rows` from `chemical_utils`
- `validate_chemical_data` from `chemical_utils`
- `parse_sampling_dates` from `updated_chemical_processing`
- `process_conditional_nutrient` from `updated_chemical_processing`
- `process_simple_nutrients` from `updated_chemical_processing`
- `format_to_database_schema` from `updated_chemical_processing`

After cleanup, the imports should be:
```python
from data_processing.chemical_utils import (
    determine_status,
    insert_collection_event,
)
```

The `updated_chemical_processing` import block can be removed entirely.

**Step 3: Run tests**

Run: `python -m pytest tests/survey123_sync/ -v --tb=short`
Expected: 18 tests pass.

**Step 4: Commit**

```bash
git add cloud_functions/survey123_sync/chemical_processor.py
git commit -m "Remove process_survey123_chemical_data from chemical_processor

Function was only used by the Survey123 auth pathway. Clean up
imports that are no longer needed by remaining functions."
```

---

### Task 5: Update `DatabaseManager` Tests

The `test_database_manager.py` tests call `get_last_sync_timestamp()` and `update_sync_timestamp()` without passing `metadata_blob_name` (relying on the old defaults). Update them to pass the blob name explicitly.

**Files:**
- Modify: `tests/survey123_sync/test_database_manager.py`

**Step 1: Update `test_get_last_sync_timestamp_exists` (line 134)**

Change `result = self.db_manager.get_last_sync_timestamp()` to:
```python
result = self.db_manager.get_last_sync_timestamp('sync_metadata/last_feature_server_sync.json')
```

Update the assertion on line 153:
```python
self.mock_bucket.blob.assert_called_with('sync_metadata/last_feature_server_sync.json')
```

**Step 2: Update `test_get_last_sync_timestamp_not_exists` (line 155)**

Change `result = self.db_manager.get_last_sync_timestamp()` to:
```python
result = self.db_manager.get_last_sync_timestamp('sync_metadata/last_feature_server_sync.json')
```

**Step 3: Update `test_get_last_sync_timestamp_error` (line 169)**

Change `result = self.db_manager.get_last_sync_timestamp()` to:
```python
result = self.db_manager.get_last_sync_timestamp('sync_metadata/last_feature_server_sync.json')
```

**Step 4: Update `test_update_sync_timestamp_success` (line 184)**

Change `result = self.db_manager.update_sync_timestamp(test_timestamp)` to:
```python
result = self.db_manager.update_sync_timestamp(test_timestamp, metadata_blob_name='sync_metadata/last_feature_server_sync.json')
```

Update the assertion on line 205:
```python
self.mock_bucket.blob.assert_called_with('sync_metadata/last_feature_server_sync.json')
```

**Step 5: Update `test_update_sync_timestamp_error` (line 207)**

Change `result = self.db_manager.update_sync_timestamp(test_timestamp)` to:
```python
result = self.db_manager.update_sync_timestamp(test_timestamp, metadata_blob_name='sync_metadata/last_feature_server_sync.json')
```

**Step 6: Run tests**

Run: `python -m pytest tests/survey123_sync/ -v --tb=short`
Expected: 18 tests pass.

**Step 7: Commit**

```bash
git add tests/survey123_sync/test_database_manager.py
git commit -m "Update DatabaseManager tests for required metadata_blob_name

Tests now pass blob name explicitly instead of relying on removed
default parameter values."
```

---

### Task 6: Update `deploy.sh`

**Files:**
- Modify: `cloud_functions/survey123_sync/deploy.sh`

**Step 1: Add TODO comment at top**

After the existing header comment, add:
```bash
# TODO: Rename function from survey123-daily-sync to blue-thumb-data-sync
# TODO: Rename cloud_functions/survey123_sync/ directory to cloud_functions/data_sync/
```

**Step 2: Remove Survey123 credential references from echo output**

Replace the "Next steps" echo block (lines 70-84) with:
```bash
echo ""
echo "Next steps:"
echo "1. Verify the function URL is configured in Cloud Scheduler"
echo "2. Ensure the SQLite database exists in Cloud Storage bucket: ${DATABASE_BUCKET}"
```

**Step 3: Remove SYNC_MODE from ENV_VARS if present**

The current `ENV_VARS` line (18) only sets `GCS_BUCKET_DATABASE` so no change needed there. Just verify.

**Step 4: Commit**

```bash
git add cloud_functions/survey123_sync/deploy.sh
git commit -m "Update deploy.sh to remove Survey123 credential references

Add TODO comments for future function/directory rename."
```

---

### Task 7: Update Comment in `arcgis_sync.py`

**Files:**
- Modify: `data_processing/arcgis_sync.py`

**Step 1: Update the comment on line 294**

Find the comment referencing `process_survey123_chemical_data()` and update it to reference `chemical_processor.py`'s remaining role or remove the reference entirely. The comment is in the `process_fetched_data` docstring:

```python
"""
Run translated DataFrame through the existing chemical processing pipeline.

This is the same sequence used by process_updated_chemical_data(),
ensuring consistent results.
```

(Remove the `process_survey123_chemical_data()` reference.)

**Step 2: Commit**

```bash
git add data_processing/arcgis_sync.py
git commit -m "Remove stale process_survey123_chemical_data reference from comment"
```

---

### Task 8: Update Documentation

**Files:**
- Modify: `docs/cloud/DEPLOYMENT.md`
- Modify: `cloud_functions/survey123_sync/README.md`
- Modify: `CLAUDE.md`
- Modify: `README.md`

**Step 1: Update `docs/cloud/DEPLOYMENT.md`**

- Remove the "Sync Modes" table (lines 79-88 with mode precedence)
- Remove "For Survey123 mode only" env vars section (lines 116-120)
- Remove `SYNC_MODE` from optional env vars
- Remove "Sync Flow (Survey123 mode)" section (lines 141-151)
- Rename "Sync Flow (FeatureServer mode)" to "Sync Flow"
- Update Cloud Function description to describe FeatureServer-only sync

**Step 2: Rewrite `cloud_functions/survey123_sync/README.md`**

Rewrite to describe the Cloud Function as a FeatureServer sync function. Remove:
- ArcGIS credential setup section
- Survey123-specific environment variables
- Survey123 authentication flow description
- Survey123 troubleshooting entries
- References to dual sync modes
- Update architecture diagram

Keep:
- Chemical processing logic description (still accurate)
- Database update flow
- Active/historic classification description
- Deployment instructions (still valid)
- Monitoring commands
- Cost estimation
- Range-based processing explanation

**Step 3: Update `CLAUDE.md`**

In the project structure section, update the `survey123_sync/` comment:
```
cloud_functions/survey123_sync/ # Daily FeatureServer data sync Cloud Function
```

Update the "Update cloud sync logic" row in Common Task Routing if it references Survey123 or dual modes.

Review "Three chemical data pathways" section — this is about CSV pathways and `arcgis_sync.py`, not the Cloud Function modes. Should be fine as-is.

**Step 4: Update root `README.md`**

- Line 21: Change `ArcGIS API` reference from "Survey123 integration" to "FeatureServer REST API for automated data sync"
- Line 38: Replace "Dual Sync Modes: Survey123 authenticated API and public ArcGIS FeatureServer" with "Automated FeatureServer Sync: Daily sync from public ArcGIS REST API with idempotent insertion"
- Line 71: Update `survey123_sync/` comment from "Automated data synchronization (Survey123 + FeatureServer modes)" to "Automated FeatureServer data sync"
- Line 72: Update "Dual-mode entry point" to "Cloud Function entry point"
- Line 157: Update "Dual Real-time Integration: Survey123 API and public ArcGIS FeatureServer sync" to "Real-time Integration: Public ArcGIS FeatureServer sync with idempotent insertion"

**Step 5: Run tests one final time**

Run: `python -m pytest tests/survey123_sync/ -v --tb=short`
Expected: 18 tests pass (docs don't affect tests, but verify nothing broke).

**Step 6: Commit**

```bash
git add docs/cloud/DEPLOYMENT.md cloud_functions/survey123_sync/README.md CLAUDE.md README.md
git commit -m "Update documentation to reflect FeatureServer-only sync

Remove all references to Survey123 auth mode, dual sync modes,
and OAuth2 credentials from deployment docs, README files, and
CLAUDE.md project guide."
```

---

### Task 9: Final Verification

**Step 1: Run full project test suite**

Run: `python -m pytest tests/ -v --tb=short`
Expected: All tests pass. No test should reference `ArcGISAuthenticator`, `Survey123DataFetcher`, `process_survey123_data`, `process_survey123_chemical_data`, or `_get_sync_mode`.

**Step 2: Grep for stale references**

```bash
grep -r "ArcGISAuthenticator\|Survey123DataFetcher\|process_survey123\|_get_sync_mode\|ARCGIS_CLIENT_ID\|ARCGIS_CLIENT_SECRET\|SURVEY123_FORM_ID" --include="*.py" --include="*.md" --include="*.sh" .
```

Expected: No matches in source code or docs (may appear in the design doc `docs/plans/` which is fine).

**Step 3: Review git log for the branch**

```bash
git log --oneline main..HEAD
```

Expected: 8 clean commits matching the tasks above.
