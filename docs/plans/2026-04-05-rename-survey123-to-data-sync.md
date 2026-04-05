# Rename survey123_sync to data_sync — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Scrub all "survey123" naming from codebase and GCP infrastructure, replacing with "data_sync" / "data-sync" to reflect the actual FeatureServer-based pipeline.

**Architecture:** Rename the directory `cloud_functions/survey123_sync/` → `cloud_functions/data_sync/`, the test directory `tests/survey123_sync/` → `tests/data_sync/`, the Cloud Function entry point `survey123_daily_sync` → `data_sync`, and the GCP function name `survey123-daily-sync` → `blue-thumb-data-sync`. Update all imports, paths, and documentation references. GCP rename requires deploying a new function and deleting the old one.

**Tech Stack:** Python, GCP Cloud Functions (gen2), Cloud Scheduler, gcloud CLI

---

### Task 1: Rename source directory and update internal references

**Files:**
- Rename: `cloud_functions/survey123_sync/` → `cloud_functions/data_sync/`
- Modify: `cloud_functions/data_sync/main.py` (entry point function name, TODOs, docstrings)
- Modify: `cloud_functions/data_sync/deploy.sh` (FUNCTION_NAME, entry-point, TODOs)
- Modify: `cloud_functions/data_sync/site_manager.py` (no survey123 refs — just needs the directory move)
- Modify: `cloud_functions/data_sync/chemical_processor.py` (no survey123 refs — just needs the directory move)
- Modify: `data_processing/arcgis_sync.py:853` (comment referencing old path)

**Step 1: Rename the directory**

```bash
git mv cloud_functions/survey123_sync cloud_functions/data_sync
```

**Step 2: Update main.py entry point and remove TODOs**

In `cloud_functions/data_sync/main.py`:

1. Remove the NOTE/TODO lines (lines 7-8):
   ```
   NOTE: Entry point name 'survey123_daily_sync' is legacy — retained for GCP config compatibility.
   TODO: Rename to 'data_sync' and update GCP function config.
   ```

2. Rename the entry point function (line 413):
   ```python
   # Before
   def survey123_daily_sync(request):
   # After
   def data_sync(request):
   ```

3. Remove the TODO in the docstring (line 421):
   ```
   NOTE: Entry point name is legacy — retained for GCP config compatibility.
   TODO: Rename to 'data_sync' and update GCP function config.
   ```

4. Update the `__main__` block (line 445):
   ```python
   # Before
   result = survey123_daily_sync(MockRequest())
   # After
   result = data_sync(MockRequest())
   ```

**Step 3: Update deploy.sh**

In `cloud_functions/data_sync/deploy.sh`:

1. Remove the two TODO lines (lines 5-6)
2. Change `FUNCTION_NAME="survey123-daily-sync"` → `FUNCTION_NAME="blue-thumb-data-sync"`
3. Change `--entry-point=survey123_daily_sync` → `--entry-point=data_sync`

**Step 4: Update arcgis_sync.py comment**

In `data_processing/arcgis_sync.py:853`, change:
```python
# Before
Note: Site resolution logic here duplicates cloud_functions/survey123_sync/site_manager.py.
# After
Note: Site resolution logic here duplicates cloud_functions/data_sync/site_manager.py.
```

**Step 5: Run tests to verify nothing broke**

Run: `pytest tests/ -x -q`
Expected: All tests pass (the test directory hasn't been renamed yet, but source imports will break — handle in Task 2)

Note: Tests will fail here because `tests/survey123_sync/` still has `sys.path` pointing to the old directory. That's expected — Task 2 fixes this.

**Step 6: Commit**

```bash
git add -A
git commit -m "refactor: rename cloud_functions/survey123_sync to data_sync"
```

---

### Task 2: Rename test directory and update test imports

**Files:**
- Rename: `tests/survey123_sync/` → `tests/data_sync/`
- Modify: `tests/data_sync/test_site_manager.py:11` (sys.path)
- Modify: `tests/data_sync/test_chemical_processor.py:17` (sys.path)
- Modify: `tests/data_sync/test_database_manager.py:16` (sys.path)
- Modify: `tests/data_sync/test_data_processing.py:17` (sys.path)
- Modify: `tests/data_sync/test_main_site_registration.py:10` (sys.path)

**Step 1: Rename the test directory**

```bash
git mv tests/survey123_sync tests/data_sync
```

**Step 2: Update sys.path in all 5 test files**

In each file, change:
```python
# Before
sys.path.insert(0, os.path.join(project_root, 'cloud_functions', 'survey123_sync'))
# After
sys.path.insert(0, os.path.join(project_root, 'cloud_functions', 'data_sync'))
```

Files to update:
- `tests/data_sync/test_site_manager.py`
- `tests/data_sync/test_chemical_processor.py`
- `tests/data_sync/test_database_manager.py`
- `tests/data_sync/test_data_processing.py`
- `tests/data_sync/test_main_site_registration.py`

**Step 3: Run the renamed tests**

Run: `pytest tests/data_sync/ -v`
Expected: All tests pass

**Step 4: Run full test suite**

Run: `pytest tests/ -x -q`
Expected: All tests pass

**Step 5: Commit**

```bash
git add -A
git commit -m "refactor: rename tests/survey123_sync to data_sync, update imports"
```

---

### Task 3: Update documentation and project config

**Files:**
- Modify: `CLAUDE.md:38` (task routing table)
- Modify: `.claude/rules/cloud.md:31,38` (cloud function location and deploy command)
- Modify: `README.md:72` (project structure)

**Step 1: Update CLAUDE.md**

Change line 38:
```markdown
# Before
| Update cloud sync | `cloud_functions/survey123_sync/main.py`, `data_processing/arcgis_sync.py` |
# After
| Update cloud sync | `cloud_functions/data_sync/main.py`, `data_processing/arcgis_sync.py` |
```

**Step 2: Update .claude/rules/cloud.md**

Change line 31:
```markdown
# Before
Located in `cloud_functions/survey123_sync/`. The directory name is legacy — retained for GCP config compatibility.
# After
Located in `cloud_functions/data_sync/`.
```

Change line 38:
```markdown
# Before
**Deploy:** `cd cloud_functions/survey123_sync && ./deploy.sh`
# After
**Deploy:** `cd cloud_functions/data_sync && ./deploy.sh`
```

**Step 3: Update README.md**

Change the project structure tree:
```markdown
# Before
│   └── survey123_sync/    # Automated FeatureServer data sync
# After
│   └── data_sync/         # Automated FeatureServer data sync
```

**Step 4: Commit**

```bash
git add CLAUDE.md .claude/rules/cloud.md README.md
git commit -m "docs: update all references from survey123_sync to data_sync"
```

---

### Task 4: Deploy new Cloud Function and update Cloud Scheduler

**This task requires manual GCP access. The user must run these commands themselves or confirm they want Claude to run them.**

**Step 1: Deploy the new function**

```bash
cd cloud_functions/data_sync && ./deploy.sh
```

This deploys `blue-thumb-data-sync` with entry point `data_sync`. Verify it succeeds and note the new function URL.

**Step 2: Update Cloud Scheduler job**

Check the current scheduler job:
```bash
gcloud scheduler jobs list --location=us-central1
```

Update the job to point to the new function URL:
```bash
gcloud scheduler jobs update http survey123-daily-sync \
    --location=us-central1 \
    --uri=<NEW_FUNCTION_URL>
```

Or, if you want to rename the scheduler job too:
```bash
# Delete old job
gcloud scheduler jobs delete survey123-daily-sync --location=us-central1

# Create new job
gcloud scheduler jobs create http blue-thumb-daily-sync \
    --location=us-central1 \
    --schedule="0 6 * * *" \
    --time-zone="America/Chicago" \
    --uri=<NEW_FUNCTION_URL> \
    --http-method=POST
```

**Step 3: Test the new function**

Trigger a manual run:
```bash
gcloud functions call blue-thumb-data-sync --region=us-central1
```

Verify it returns a success response with records fetched.

**Step 4: Delete the old Cloud Function**

Only after confirming the new function works:
```bash
gcloud functions delete survey123-daily-sync --region=us-central1 --gen2
```

**Step 5: Commit any remaining changes**

```bash
git add -A
git commit -m "chore: complete survey123 → data_sync rename"
```
