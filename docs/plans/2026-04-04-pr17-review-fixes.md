# PR #17 Review Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Address Miguel's code review feedback on PR #17 — rebase onto main, integrate the 5-step site resolution chain, fix INSERT OR IGNORE data loss, fix promotion transaction safety, and replace the tautology test.

**Architecture:** The core change is merging main's name-based site resolution (`_resolve_site` with exact → normalized → alias) with this branch's coordinate-based resolution (`resolve_unknown_site` with Haversine + pending_sites staging). The result is a 5-step resolution chain in `site_manager.py`, keeping `chemical_processor.py` as a thin caller. Transaction safety is fixed by deferring `promote_approved_sites`'s commit to the caller.

**Tech Stack:** Python, SQLite, pytest

---

### Task 1: Rebase onto main

This is the prerequisite for everything. The branch has diverged — 6 commits on main edited `chemical_processor.py` (adding `_resolve_site`, `SITE_ALIASES`, `normalize_site_name`, unknown-site tracking) and 4 commits on this branch also edited it (replacing all of that with `site_manager.py`).

**Step 1: Rebase**

```bash
git fetch origin
git rebase origin/main
```

**Step 2: Resolve conflicts in `chemical_processor.py`**

When the conflict appears, take **this branch's version** as the base (the one using `site_manager.py`). The name-based resolution logic from main will be moved into `site_manager.py` in Task 2, not kept in `chemical_processor.py`.

Specifically:
- Keep our imports: `from site_manager import resolve_unknown_site` (not main's `SITE_ALIASES`, `normalize_site_name`)
- Keep our `insert_processed_data_to_db` loop structure (the one using `resolve_unknown_site` + `resolved_cache`)
- Discard main's `_resolve_site` inner function, `normalized_site_lookup`, `unknown_site_counts`, `unknown_site_sample_ids` — all of this moves to `site_manager.py`

**Step 3: Run tests to verify rebase succeeded**

```bash
pytest tests/survey123_sync/ -v
```

**Step 4: Commit the rebase resolution**

The rebase will auto-commit on success. Verify with `git log --oneline -5`.

---

### Task 2: Integrate 5-step site resolution into `site_manager.py` (Points 1+2)

Move main's name resolution logic into `resolve_unknown_site` so the chain is:
1. Exact match
2. Normalized (casefold + whitespace)
3. Alias lookup
4. Haversine coordinate match
5. Stage to `pending_sites`

**Files:**
- Modify: `cloud_functions/survey123_sync/site_manager.py:1-91`
- Modify: `cloud_functions/survey123_sync/chemical_processor.py:102-137` (pass new args)
- Test: `tests/survey123_sync/test_site_manager.py`

**Step 1: Write failing tests for name resolution steps**

Add to `tests/survey123_sync/test_site_manager.py`, in `TestResolveUnknownSite`:

```python
def test_normalized_match_returns_site_id(self):
    """Site matching by normalized name should return existing site_id."""
    from site_manager import resolve_unknown_site

    existing = self._get_existing_sites()
    site_lookup = {'Bull Creek: Main': 1, 'Clear Creek: Bridge': 2}
    # Extra whitespace + trailing period — should normalize to match
    result = resolve_unknown_site(
        'Bull  Creek:  Main.', None, None, existing, self.conn,
        site_lookup=site_lookup,
    )
    self.assertEqual(result, 1)

def test_alias_match_returns_site_id(self):
    """Site matching via SITE_ALIASES should return existing site_id."""
    from site_manager import resolve_unknown_site

    existing = self._get_existing_sites()
    # Add a site that's the canonical name for an alias
    cursor = self.conn.cursor()
    cursor.execute(
        "INSERT INTO sites (site_id, site_name, latitude, longitude) "
        "VALUES (3, 'Cow Creek: West Virginia Avenue', 35.6, -97.3)"
    )
    self.conn.commit()
    site_lookup = {
        'Bull Creek: Main': 1, 'Clear Creek: Bridge': 2,
        'Cow Creek: West Virginia Avenue': 3,
    }
    # 'cow creek: virginia avenue' is an alias for 'Cow Creek: West Virginia Avenue'
    result = resolve_unknown_site(
        'Cow Creek: Virginia Avenue', None, None, existing, self.conn,
        site_lookup=site_lookup,
    )
    self.assertEqual(result, 3)
```

**Step 2: Run tests to verify they fail**

```bash
pytest tests/survey123_sync/test_site_manager.py::TestResolveUnknownSite::test_normalized_match_returns_site_id -v
pytest tests/survey123_sync/test_site_manager.py::TestResolveUnknownSite::test_alias_match_returns_site_id -v
```

Expected: FAIL (resolve_unknown_site doesn't accept `site_lookup` kwarg yet)

**Step 3: Update `resolve_unknown_site` signature and add name resolution**

In `cloud_functions/survey123_sync/site_manager.py`, update the function:

```python
from data_processing.chemical_utils import (
    SITE_ALIASES,
    normalize_site_name,
)

def resolve_unknown_site(site_name, latitude, longitude, existing_sites, conn,
                         site_lookup=None):
    """Resolve an unknown site name against existing sites.

    Resolution chain:
    1. Normalized name match (casefold + whitespace normalization)
    2. Alias lookup via SITE_ALIASES
    3. Haversine coordinate match (within 50m)
    4. Stage to pending_sites if all else fails

    Args:
        site_name: The unknown site name.
        latitude: Latitude from FeatureServer geometry (may be None).
        longitude: Longitude from FeatureServer geometry (may be None).
        existing_sites: List of (site_id, site_name, lat, lon) tuples.
        conn: SQLite connection (caller manages transaction).
        site_lookup: Optional dict of {site_name: site_id} for name-based resolution.

    Returns:
        site_id if resolved, None if staged as pending.
    """
    # Step 1: Normalized name match
    if site_lookup:
        normalized_key = normalize_site_name(site_name).casefold()
        for db_name, db_id in site_lookup.items():
            if normalize_site_name(db_name).casefold() == normalized_key:
                logger.info(
                    f"Normalized match: '{site_name}' → '{db_name}' (site_id={db_id})"
                )
                return db_id

        # Step 2: Alias lookup
        canonical_name = SITE_ALIASES.get(normalized_key)
        if canonical_name:
            site_id = site_lookup.get(canonical_name)
            if site_id is None:
                # Try normalized lookup of canonical name
                canonical_norm = normalize_site_name(canonical_name).casefold()
                for db_name, db_id in site_lookup.items():
                    if normalize_site_name(db_name).casefold() == canonical_norm:
                        site_id = db_id
                        break
            if site_id is not None:
                logger.info(
                    f"Alias match: '{site_name}' → '{canonical_name}' (site_id={site_id})"
                )
                return site_id

    # Step 3: Haversine coordinate match
    nearest_name = None
    nearest_dist = float('inf')

    if latitude is not None and longitude is not None:
        for site_id, existing_name, ex_lat, ex_lon in existing_sites:
            if ex_lat is None or ex_lon is None:
                continue
            dist = haversine_m(latitude, longitude, ex_lat, ex_lon)
            if dist <= DISTANCE_THRESHOLD_M:
                logger.info(
                    f"Coordinate match: '{site_name}' is {dist:.1f}m from "
                    f"existing site '{existing_name}' (site_id={site_id})"
                )
                return site_id
            if dist < nearest_dist:
                nearest_dist = dist
                nearest_name = existing_name

    # Step 4: Stage as pending
    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    cursor.execute(
        """
        INSERT INTO pending_sites
            (site_name, latitude, longitude, first_seen_date, source, status,
             nearest_site_name, nearest_site_distance_m)
        VALUES (?, ?, ?, ?, 'feature_server', 'pending', ?, ?)
        ON CONFLICT(site_name) DO UPDATE SET
            latitude = COALESCE(excluded.latitude, pending_sites.latitude),
            longitude = COALESCE(excluded.longitude, pending_sites.longitude),
            nearest_site_name = excluded.nearest_site_name,
            nearest_site_distance_m = excluded.nearest_site_distance_m
        """,
        (
            site_name,
            latitude,
            longitude,
            today,
            nearest_name,
            nearest_dist if nearest_dist != float('inf') else None,
        ),
    )
    if cursor.rowcount > 0:
        logger.info(
            f"Staged new pending site: '{site_name}' "
            f"(nearest: '{nearest_name}' at {nearest_dist:.0f}m)"
            if nearest_name
            else f"Staged new pending site: '{site_name}' (no coordinates for distance check)"
        )
    return None
```

Note: This also addresses **Point 3** (INSERT OR IGNORE → ON CONFLICT DO UPDATE) in the same change.

**Step 4: Update `chemical_processor.py` to pass `site_lookup`**

In `insert_processed_data_to_db`, update the call at ~line 129:

```python
                    resolved_id = resolve_unknown_site(
                        site_name, lat, lon, coord_rows, conn,
                        site_lookup=site_lookup,
                    )
```

**Step 5: Run all tests**

```bash
pytest tests/survey123_sync/test_site_manager.py -v
pytest tests/survey123_sync/ -v
```

Expected: All pass.

**Step 6: Commit**

```bash
git add cloud_functions/survey123_sync/site_manager.py cloud_functions/survey123_sync/chemical_processor.py tests/survey123_sync/test_site_manager.py
git commit -m "feat: 5-step site resolution chain (name → alias → coords → pending)"
```

---

### Task 3: Fix promotion transaction safety (Point 4)

**Problem:** `promote_approved_sites` commits on its own connection. If chemical insertion fails later, `upload_database` never runs and the promoted sites are lost from GCS.

**Fix:** Remove the internal `conn.commit()` from `promote_approved_sites`. Let the caller manage the transaction. In `main.py`, only commit after the full pipeline succeeds (or at least ensure upload happens even if insertion fails).

**Files:**
- Modify: `cloud_functions/survey123_sync/site_manager.py:122-128`
- Modify: `cloud_functions/survey123_sync/main.py:166-191`
- Test: `tests/survey123_sync/test_site_manager.py`

**Step 1: Write a test that verifies promote doesn't auto-commit**

Add to `TestPromoteApprovedSites` in `test_site_manager.py`:

```python
def test_promote_does_not_auto_commit(self):
    """promote_approved_sites should not commit — caller manages transaction."""
    from site_manager import promote_approved_sites

    cursor = self.conn.cursor()
    cursor.execute(
        "INSERT INTO pending_sites (site_name, latitude, longitude, first_seen_date, status) "
        "VALUES ('Auto Commit Test', 35.5, -97.2, '2026-04-01', 'approved')"
    )
    self.conn.commit()

    promote_approved_sites(self.conn)

    # Open a separate connection to check — if promote committed,
    # the new site will be visible. If not, it won't be (since we
    # haven't committed on self.conn after promote).
    check_conn = sqlite3.connect(self.temp_db.name)
    cursor2 = check_conn.cursor()
    cursor2.execute("SELECT COUNT(*) FROM sites WHERE site_name = 'Auto Commit Test'")
    count = cursor2.fetchone()[0]
    check_conn.close()

    # Should NOT be visible yet — caller hasn't committed
    self.assertEqual(count, 0, "promote_approved_sites should not auto-commit")
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/survey123_sync/test_site_manager.py::TestPromoteApprovedSites::test_promote_does_not_auto_commit -v
```

Expected: FAIL (promote currently calls `conn.commit()`)

**Step 3: Remove auto-commit from `promote_approved_sites`**

In `site_manager.py`, remove `conn.commit()` at line 128 and update the docstring:

```python
def promote_approved_sites(conn):
    """Move approved pending sites into the sites table.

    Args:
        conn: SQLite connection. Caller is responsible for committing.

    Returns:
        Dict with 'promoted' count and 'names' list.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT site_name, latitude, longitude FROM pending_sites WHERE status = 'approved'"
    )
    approved = cursor.fetchall()

    promoted_names = []
    for site_name, lat, lon in approved:
        cursor.execute(
            """
            INSERT OR IGNORE INTO sites (site_name, latitude, longitude, active)
            VALUES (?, ?, ?, 1)
            """,
            (site_name, lat, lon),
        )
        if cursor.rowcount > 0:
            promoted_names.append(site_name)
            logger.info(f"Promoted pending site to sites table: '{site_name}'")

    if promoted_names:
        cursor.execute(
            "UPDATE pending_sites SET status = 'promoted', reviewed_date = ? "
            "WHERE status = 'approved'",
            (datetime.now().strftime('%Y-%m-%d'),),
        )

    return {'promoted': len(promoted_names), 'names': promoted_names}
```

**Step 4: Update `main.py` to commit after promotion**

In `main.py` around line 184-191, add an explicit commit after calling promote:

```python
            promote_result = promote_approved_sites(promote_conn)
            promote_conn.commit()  # Explicit commit — caller manages transaction
            if promote_result['promoted'] > 0:
                logger.info(
                    f"Promoted {promote_result['promoted']} approved sites: "
                    f"{promote_result['names']}"
                )
```

**Step 5: Fix existing tests that relied on auto-commit**

The existing `test_approved_site_promoted` and `test_promoted_site_status_updated` tests call `promote_approved_sites` and then check the DB. Since promote no longer commits, add `self.conn.commit()` after the promote call in those tests.

**Step 6: Run tests**

```bash
pytest tests/survey123_sync/test_site_manager.py -v
```

Expected: All pass.

**Step 7: Commit**

```bash
git add cloud_functions/survey123_sync/site_manager.py cloud_functions/survey123_sync/main.py tests/survey123_sync/test_site_manager.py
git commit -m "fix: remove auto-commit from promote_approved_sites, caller manages transaction"
```

---

### Task 4: Replace tautology test (Point 6)

**Problem:** `test_main_pending_sites.py` mocks `_run_feature_server_sync`, calls the mock directly, and asserts on the mock's return value. Tests nothing.

**Fix:** Replace with a test that verifies `_run_feature_server_sync` actually includes pending_sites data in its response by testing the real orchestration with a stubbed DB and mocked external dependencies (GCS, FeatureServer).

**Files:**
- Rewrite: `tests/survey123_sync/test_main_pending_sites.py`

**Step 1: Rewrite the test**

```python
"""Tests for pending site orchestration in main.py."""
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'cloud_functions', 'survey123_sync'))

sys.modules['functions_framework'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()


class TestPendingSitesOrchestration(unittest.TestCase):
    """Verify pending_sites lifecycle is wired into the sync pipeline."""

    def test_promote_called_before_data_insertion(self):
        """promote_approved_sites should run before insert_processed_data_to_db."""
        from datetime import datetime
        call_order = []

        mock_db_manager = MagicMock()
        mock_db_manager.download_database.side_effect = lambda path: _seed_test_db(path)
        mock_db_manager.bucket = MagicMock()
        mock_db_manager.bucket.blob.return_value.exists.return_value = False
        mock_db_manager.upload_database.return_value = True
        mock_db_manager.update_sync_timestamp.return_value = True

        def track_promote(conn):
            call_order.append('promote')
            return {'promoted': 0, 'names': []}

        def track_insert(df, db_path):
            call_order.append('insert')
            return {'records_inserted': 0}

        with patch('main.arcgis_sync') as mock_arcgis, \
             patch('main.promote_approved_sites', side_effect=track_promote), \
             patch('main.insert_processed_data_to_db', side_effect=track_insert), \
             patch('main.classify_active_sites_in_db', return_value={'active_count': 0, 'historic_count': 0}), \
             patch('main.get_pending_site_summary', return_value={'total_pending': 0}):

            mock_arcgis.fetch_features_since.return_value = [{'some': 'record'}]
            mock_arcgis.translate_to_pipeline_schema.return_value = pd.DataFrame({'col': [1]})
            mock_arcgis.process_fetched_data.return_value = pd.DataFrame({'col': [1]})

            from main import _run_feature_server_sync
            _run_feature_server_sync(mock_db_manager, datetime.now())

        self.assertEqual(call_order, ['promote', 'insert'])


def _seed_test_db(path):
    """Create a minimal DB so the function doesn't crash on missing tables."""
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS sites (site_id INTEGER PRIMARY KEY, site_name TEXT)')
    cursor.execute('''CREATE TABLE IF NOT EXISTS pending_sites (
        pending_site_id INTEGER PRIMARY KEY, site_name TEXT NOT NULL,
        latitude REAL, longitude REAL, first_seen_date TEXT NOT NULL,
        source TEXT, status TEXT, reviewed_date TEXT, notes TEXT,
        nearest_site_name TEXT, nearest_site_distance_m REAL, UNIQUE(site_name))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS chemical_collection_events (
        event_id INTEGER PRIMARY KEY, collection_date TEXT)''')
    conn.commit()
    conn.close()
    return True


if __name__ == '__main__':
    unittest.main()
```

**Step 2: Run the test**

```bash
pytest tests/survey123_sync/test_main_pending_sites.py -v
```

Expected: PASS — verifies promote runs before insert in the real orchestration flow.

Note: This test may need adjustment based on how main.py imports after the rebase. The key assertion is that promote is called before insert — verify the patch targets match the actual import paths.

**Step 3: Commit**

```bash
git add tests/survey123_sync/test_main_pending_sites.py
git commit -m "test: replace tautology test with real orchestration verification"
```

---

### Task 5: Run full test suite and verify

**Step 1: Run all cloud sync tests**

```bash
pytest tests/survey123_sync/ -v
```

**Step 2: Run full test suite**

```bash
pytest
```

**Step 3: Verify no regressions, fix any failures**

---

## Summary of Changes by File

| File | What changes |
|------|-------------|
| `site_manager.py` | Add normalized + alias resolution before Haversine; change INSERT OR IGNORE → ON CONFLICT DO UPDATE; remove auto-commit from promote |
| `chemical_processor.py` | Pass `site_lookup` to `resolve_unknown_site`; resolve rebase conflict favoring our structure |
| `main.py` | Add explicit `promote_conn.commit()` after promote call |
| `test_site_manager.py` | Add tests for normalized match, alias match, no-auto-commit; update existing tests for caller-managed commit |
| `test_main_pending_sites.py` | Full rewrite — test real orchestration order instead of mock tautology |
