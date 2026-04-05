# Simplify Site Registration: Drop pending_sites, Auto-Insert New Sites

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the human-in-the-loop `pending_sites` workflow with auto-insertion of new sites directly into the `sites` table, since Kim's QAQC sign-off on the FeatureServer is sufficient verification.

**Architecture:** Keep the existing resolution chain (normalized name → alias → Haversine 50m coords) but change the final fallback from "stage to pending_sites" to "INSERT INTO sites." Remove the `pending_sites` table, promotion workflow, and all associated orchestration. Cherry-pick the `@functions_framework.http` decorator fix from PR #15.

**Tech Stack:** Python, SQLite, pytest

---

### Task 1: Cherry-pick decorator fix

**Files:**
- Modify: `cloud_functions/survey123_sync/main.py:456` (add decorator)

**Step 1: Add the `@functions_framework.http` decorator**

```python
# Line 456 currently reads:
def survey123_daily_sync(request):

# Change to:
@functions_framework.http
def survey123_daily_sync(request):
```

**Step 2: Run existing tests to verify no breakage**

Run: `pytest tests/survey123_sync/ -v`
Expected: All pass

**Step 3: Commit**

```bash
git add cloud_functions/survey123_sync/main.py
git commit -m "fix: restore @functions_framework.http decorator on entry point

Cherry-picked from PR #15 (6f56da2). Decorator was lost in PR #12 merge
conflict resolution. Not a production bug (deploy.sh --entry-point handles it)
but enables local dev without the flag."
```

---

### Task 2: Change site_manager to auto-insert into sites table

**Files:**
- Modify: `cloud_functions/survey123_sync/site_manager.py`
- Modify: `tests/survey123_sync/test_site_manager.py`

**Step 1: Update test expectations — change "stages to pending" tests to "auto-inserts into sites"**

In `test_site_manager.py`:

- `test_no_match_inserts_pending` → rename to `test_no_match_auto_inserts_site`. Assert the site appears in `sites` table (not `pending_sites`). Assert `resolve_unknown_site` returns the new `site_id`.
- `test_pending_records_nearest_site` → delete (nearest site tracking was for pending review context)
- `test_duplicate_pending_ignored` → rename to `test_duplicate_auto_insert_ignored`. Assert second call doesn't create a duplicate in `sites`, returns same `site_id`.
- `test_no_coordinates_inserts_pending` → rename to `test_no_coordinates_auto_inserts_site`. Assert site goes into `sites` with NULL coords.
- `test_coordinate_update_on_conflict` → delete (was for pending_sites coord backfill)
- Delete entire `TestPromoteApprovedSites` class
- Remove `pending_sites` table creation from `_create_test_db`

**Step 2: Run tests to verify they fail**

Run: `pytest tests/survey123_sync/test_site_manager.py -v`
Expected: FAIL (site_manager still writes to pending_sites)

**Step 3: Rewrite `site_manager.py`**

- `resolve_unknown_site` Step 4: replace `INSERT INTO pending_sites` with `INSERT OR IGNORE INTO sites (site_name, latitude, longitude, active) VALUES (?, ?, ?, 1)`. Query back the `site_id` and return it (never return None).
- Delete `promote_approved_sites` function
- Delete `get_pending_site_summary` function
- Update module docstring

**Step 4: Run tests to verify they pass**

Run: `pytest tests/survey123_sync/test_site_manager.py -v`
Expected: All pass

**Step 5: Commit**

```bash
git add cloud_functions/survey123_sync/site_manager.py tests/survey123_sync/test_site_manager.py
git commit -m "feat: auto-insert new sites instead of staging to pending_sites

QAQC-complete sign-off on the FeatureServer is sufficient verification.
Sites that pass the resolution chain (name → alias → Haversine 50m)
are genuinely new and can be inserted directly."
```

---

### Task 3: Update chemical_processor to handle auto-inserted sites

**Files:**
- Modify: `cloud_functions/survey123_sync/chemical_processor.py`
- Modify: `tests/survey123_sync/test_chemical_processor.py`

**Step 1: Update test — unknown site should now get data inserted (not skipped)**

In `test_chemical_processor.py`:
- `test_unknown_site_no_match_goes_to_pending` → rename to `test_unknown_site_no_match_auto_inserted`. Remove `pending_sites` table creation from test setup. Assert `records_inserted > 0` (not 0). Assert the site exists in `sites` table. Remove assertion on `pending_sites`.
- Remove `pending_sites` table creation from `test_unknown_site_with_coord_match_gets_inserted` setup.

**Step 2: Run tests to verify they fail**

Run: `pytest tests/survey123_sync/test_chemical_processor.py::TestUnknownSiteResolution -v`
Expected: FAIL

**Step 3: Update `chemical_processor.py`**

- `resolve_unknown_site` now always returns a `site_id` (auto-insert means it never returns None)
- Remove the `if resolved_id is None: continue` skip + `new_pending_names` tracking
- Remove `new_pending` from result dict
- Keep `coordinate_matched` counter
- Add `new_sites_created` counter for sites auto-inserted by the resolution chain

**Step 4: Run tests to verify they pass**

Run: `pytest tests/survey123_sync/test_chemical_processor.py -v`
Expected: All pass

**Step 5: Commit**

```bash
git add cloud_functions/survey123_sync/chemical_processor.py tests/survey123_sync/test_chemical_processor.py
git commit -m "feat: chemical processor inserts data for auto-created sites

resolve_unknown_site now always returns a site_id, so no data is ever
skipped for unknown sites. Removes new_pending tracking."
```

---

### Task 4: Simplify main.py orchestration

**Files:**
- Modify: `cloud_functions/survey123_sync/main.py`
- Modify: `tests/survey123_sync/test_main_pending_sites.py`

**Step 1: Update orchestration test**

Rename `test_main_pending_sites.py` → `test_main_site_registration.py`. Remove:
- `pending_sites` table creation from `_seed_test_db`
- Import/mock of `promote_approved_sites` and `get_pending_site_summary`
- Assertion on `pending_sites` in result
- `call_order` tracking for promote (no longer exists)

Replace with: assert result contains `new_sites_created` count.

**Step 2: Run test to verify it fails**

Run: `pytest tests/survey123_sync/test_main_site_registration.py -v`
Expected: FAIL

**Step 3: Update `main.py`**

- Remove import of `promote_approved_sites`, `get_pending_site_summary` from `site_manager`
- Remove the `pending_sites` CREATE TABLE IF NOT EXISTS block
- Remove `promote_conn` connection + promote_approved_sites call + commit
- Remove `pending_conn` connection + `get_pending_site_summary` call
- Remove `pending_sites` block from response dict
- Remove `pending_sites_promoted` and `new_pending_sites` from sync metadata
- Replace `new_pending` backfill logic with `new_sites_created` from `insert_result`
- Keep backfill logic but key off `new_sites_created` instead of `new_pending`

**Step 4: Run tests to verify they pass**

Run: `pytest tests/survey123_sync/ -v`
Expected: All pass

**Step 5: Commit**

```bash
git add cloud_functions/survey123_sync/main.py tests/survey123_sync/test_main_pending_sites.py tests/survey123_sync/test_main_site_registration.py
git commit -m "refactor: remove pending_sites orchestration from sync pipeline

No more promotion step, no pending_site_summary. Sites are auto-inserted
by site_manager during chemical processing."
```

---

### Task 5: Remove pending_sites from database schema

**Files:**
- Modify: `database/db_schema.py:283-299`
- Delete: `tests/database/test_pending_sites_schema.py`

**Step 1: Remove the pending_sites table from `create_tables()`**

Delete lines 283-299 in `db_schema.py` (the `-- PENDING SITES TABLE --` section).

**Step 2: Delete `test_pending_sites_schema.py`**

**Step 3: Run full test suite**

Run: `pytest -v`
Expected: All pass

**Step 4: Commit**

```bash
git add database/db_schema.py
git rm tests/database/test_pending_sites_schema.py
git commit -m "chore: remove pending_sites table from schema

Table is no longer needed — sites are auto-inserted directly."
```

---

### Task 6: Final verification

**Step 1: Run full test suite**

Run: `pytest -v`
Expected: All pass, no references to pending_sites remaining

**Step 2: Grep for any remaining pending_sites references**

Run: `grep -r "pending_sites" --include="*.py" .`
Expected: No matches (or only in git history / plan docs)

**Step 3: Verify the branch diff looks clean**

Run: `git diff origin/main --stat`

**Step 4: Update PR description on GitHub**

Update PR #17 body to reflect the simplified approach: auto-insert instead of pending_sites.
