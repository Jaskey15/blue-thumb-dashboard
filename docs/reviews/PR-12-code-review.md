# PR #12 Code Review Findings

**PR:** Harden FeatureServer ingest + backfill metadata; fix GCS DB refresh
**Branch:** `fix/feature-server-sync-robust`
**Files changed:** `chemical_processor.py`, `main.py`, `database.py`, `test_data_processing.py`
**Date reviewed:** 2026-02-22
**Automated threshold:** None of the 7 issues scored >= 80/100 (the threshold for automated posting)

---

## Summary

PR #12 hardens the FeatureServer realtime ingest pipeline with deterministic site resolution, unknown-site reporting with sample_ids, watermark/backfill safety, a GCS blob generation fix, and enhanced logging. Five independent review passes identified 7 issues — all scored 75 or below, meaning they are worth discussion but did not meet the bar for automated comment posting.

---

## Issues Found

### 1. `site_aliases` placed in `chemical_processor.py` instead of `chemical_utils.py` (Score: 75)

**Type:** CLAUDE.md adherence
**File:** `cloud_functions/survey123_sync/chemical_processor.py`

The `site_aliases` dictionary is hardcoded inside `insert_processed_data_to_db()`. The CLAUDE.md routing table says chemical constants belong in `chemical_utils.py`, and notes that all three chemical data pathways share that module. Embedding aliases in one processor makes them invisible to the other two pathways (`chemical_processing.py`, `updated_chemical_processing.py`).

**CLAUDE.md references:**
- "Add/change a chemical parameter | `chemical_utils.py` (constants)"
- "Three chemical data pathways... All share `chemical_utils.py`"

**Recommendation:** Move `site_aliases` and `_normalize_site_name()` to `chemical_utils.py`.

---

### 2. Silent no-op exception handling in `_get_feature_server_override` (Score: 75)

**Type:** Bug / pattern violation
**File:** `cloud_functions/survey123_sync/main.py`

Both `except Exception` blocks contain only self-assignments (`since_date = since_date`). These are no-ops that silently swallow exceptions without logging. Every other exception handler in `main.py` calls `logger.warning()` or `logger.error()`. Silent swallowing makes debugging harder in an unattended cloud function.

```python
except Exception:
    since_date = since_date       # no-op
    since_datetime = since_datetime  # no-op
```

**Recommendation:** Either log the exception at `logger.debug()` level or replace the self-assignments with `pass` to make the intent explicit.

---

### 3. Test schema missing FK constraint (Score: 75)

**Type:** CLAUDE.md adherence
**File:** `tests/survey123_sync/test_data_processing.py`

The `_create_minimal_db` helper creates `chemical_collection_events` without a `FOREIGN KEY (site_id) REFERENCES sites(site_id)` constraint. The CLAUDE.md says "Foreign keys enforced" and "all processing tables have foreign keys to `sites`." Other test files (`test_chemical_processor.py`, `test_chemical_processing.py`) include the FK constraint. Without it, a bug that inserts a wrong `site_id` wouldn't be caught by these tests.

**Recommendation:** Add the FK constraint to the test schema and enable `PRAGMA foreign_keys = ON`.

---

### 4. Backup failure prevents primary DB upload (Score: 75)

**Type:** Bug
**File:** `cloud_functions/survey123_sync/main.py`, `upload_database()`

The backup step is wrapped in try/except that re-raises on failure. This means a transient GCS error on the backup path (a non-critical operation) prevents the primary database upload from ever executing. All processed data for that sync cycle is lost. The PR restructures the error handling but preserves this behavior from the original code.

```python
try:
    backup_blob.upload_from_string(blob.download_as_string())
except Exception as e:
    logger.error(...)
    raise  # <-- prevents primary upload from executing
```

**Recommendation:** Catch and log backup failures without re-raising, then proceed with the primary upload. The backup is best-effort; the primary upload is critical.

---

### 5. Alias misconfiguration is invisible in diagnostics (Score: 25)

**Type:** Bug (low probability)
**File:** `cloud_functions/survey123_sync/chemical_processor.py`

If a site alias's canonical target name doesn't exist in the database (e.g., typo in the alias value), the record silently falls through to the "unknown site" path. The diagnostic output reports the original unresolved name, not the canonical name — so there's no signal that an alias matched but its target wasn't found.

**Recommendation:** Log a distinct warning when an alias matches but the canonical lookup fails, so misconfigured aliases are detectable.

---

### 6. Invalid `since_datetime_override` crashes sync (Score: 75)

**Type:** Bug
**File:** `cloud_functions/survey123_sync/main.py`

When an operator passes an unparseable `since_datetime` value (e.g., `?since_datetime=bad-value`), the exception handler assigns the raw string. This raw string flows to `arcgis_sync.fetch_features_edited_since()`, which calls `int(since_datetime)` on non-datetime inputs — raising an uncaught `ValueError` that crashes the entire sync with a 500 error.

**Code path:**
1. `_get_feature_server_override()` catches parse failure, keeps raw string
2. `_run_feature_server_sync()` passes raw string to `fetch_features_edited_since()`
3. `arcgis_sync.py` line 148: `int(since_datetime)` raises `ValueError`

**Recommendation:** Validate the override value more strictly — if ISO parse fails, discard it and log a warning rather than passing the raw string downstream.

---

### 7. `day_backfill` infinite loop with persistent unknown sites (Score: 75)

**Type:** Bug
**File:** `cloud_functions/survey123_sync/main.py`

When `sync_strategy='day_backfill'` and records are skipped due to unknown sites, the code writes `needs_backfill=True` with the same `backfill_since_date` back to metadata. On the next daily invocation, it picks `day_backfill` again with the same date and re-fetches the same range. If the unknown sites are never resolved, this repeats indefinitely — each time downloading and processing the same FeatureServer data. Data integrity is safe (sample_id idempotency), but there's no maximum-retry count or "give up and advance" mechanism.

**Recommendation:** Add a `backfill_attempt_count` to metadata and cap retries (e.g., 7 days). After the cap, advance the watermark and log a final warning with the unresolved site names and sample_ids.

---

## Non-Issues Verified

These were investigated and confirmed to not be problems:

- **Column names** (`Site_Name`, `Date`, etc.) in DataFrames are pre-existing conventions across all three pathways, not raw input columns
- **`sqlite3.connect()` in Cloud Function** instead of `get_connection()` is the established pattern for CF temp DBs
- **`database.py` blob fix** (`get_blob()` replacing `blob.reload()`) is a correct improvement
- **Test queries without `data_queries.py`** — test code tests CF internals, not dashboard retrieval
- **Previous PR #4 issues** — all three issues from that review (INSERT OR IGNORE, events_added counter, temp file leak) are fully resolved in this PR
- **`_ensure_gcp_db_ready` still uses `blob.reload()`** — runs only once at startup, stale-blob concern doesn't apply
