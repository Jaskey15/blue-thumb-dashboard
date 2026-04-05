# PR #17 Merge Conflict Resolution Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rebase PR #17 (cloud site registration) onto current `origin/main` (which includes PR #16's API-first pipeline), resolving 3 merge conflicts and fixing the disabled Haversine dedup during sync.

**Architecture:** Take main's `return_geometry` parameter approach in `arcgis_sync.py`, pass `return_geometry=True` from both sync fetch calls so coordinates flow through the pipeline, extract lat/lon in `prepare_dataframe` so `chemical_processor` can use them for Haversine matching. Delete `updated_chemical_processing.py` (already deleted on main). Keep PR #17's module-level `haversine_m` in `merge_sites.py` alongside main's `load_reference_data(conn)`.

**Tech Stack:** Python, SQLite, Pandas, ArcGIS Feature Server API

---

## Context

### What PR #16 changed (now on main)
- `arcgis_sync.py` was rewritten: uses API field names directly, has `prepare_dataframe()` (not `translate_to_pipeline_schema()`), `_fetch_features_paginated` takes `return_geometry` param (default `False`), returns full feature dicts when `True`
- `updated_chemical_processing.py` was deleted — Feature Server replaced the CSV pipeline
- `merge_sites.py` renamed `load_csv_files()` → `load_reference_data(conn)`, queries DB for Feature Server sites instead of reading the now-deleted CSV
- `chemical_processor.py` on main has `_resolve_site()` with exact → normalized → alias matching, tracks unknown sites, but does NOT have Haversine or auto-insert (it skips unknown sites)

### What PR #17 adds (this branch)
- `site_manager.py` — 4-step resolution: normalized → alias → Haversine → auto-insert
- `chemical_processor.py` — simplified version that delegates to `site_manager.resolve_unknown_site`, tracks `new_sites_created`
- `main.py` — simplified metadata (no unknown_sites tracking, since sites are auto-inserted)
- `merge_sites.py` — extracted `haversine_m` to module level
- `arcgis_sync.py` — hardcoded `returnGeometry: 'true'`, injected lat/lon into attributes

### Merge strategy
We merge `origin/main` INTO this branch (not rebase), resolve conflicts, and the result is the PR diff.

---

### Task 1: Merge origin/main and resolve arcgis_sync.py conflict

**Files:**
- Modify: `data_processing/arcgis_sync.py`

**Step 1: Start the merge**

```bash
git merge origin/main --no-commit
```

This will produce CONFLICT in 3 files. We resolve them one at a time.

**Step 2: Take main's arcgis_sync.py entirely, then add geometry passthrough**

Main's `arcgis_sync.py` is the authoritative version (API-first pipeline). Take it wholesale:

```bash
git checkout origin/main -- data_processing/arcgis_sync.py
```

**Step 3: Add `return_geometry=True` to `fetch_features_since`**

In `data_processing/arcgis_sync.py`, find `fetch_features_since` (~line 338-356). Its call to `_fetch_features_paginated` needs `return_geometry=True`:

```python
    return _fetch_features_paginated(
        where=where,
        out_fields=OUT_FIELDS,
        order_by_fields='day DESC',
        timeout_seconds=timeout_seconds,
        return_geometry=True,
    )
```

**Step 4: Add `return_geometry=True` to both calls in `fetch_features_edited_since`**

In `fetch_features_edited_since` (~line 359-386), there are two calls to `_fetch_features_paginated` (the epoch try and the timestamp fallback). Add `return_geometry=True` to both:

First call (epoch-based):
```python
        return _fetch_features_paginated(
            where=where_epoch,
            out_fields=out_fields,
            order_by_fields='EditDate DESC',
            timeout_seconds=timeout_seconds,
            return_geometry=True,
        )
```

Second call (timestamp fallback):
```python
        return _fetch_features_paginated(
            where=where_ts,
            out_fields=out_fields,
            order_by_fields='EditDate DESC',
            timeout_seconds=timeout_seconds,
            return_geometry=True,
        )
```

**Step 5: Update `prepare_dataframe` to extract lat/lon from geometry**

`prepare_dataframe` (~line 438-477) currently creates a DataFrame directly from records. When `return_geometry=True`, records are full feature dicts (with `attributes` and `geometry` keys), not flat attribute dicts. Update to handle both formats and extract lat/lon:

Replace the body of `prepare_dataframe` with:

```python
def prepare_dataframe(records):
    """
    Convert raw Feature Server records into a flat DataFrame.

    Handles both attribute-only dicts (return_geometry=False) and full
    feature dicts (return_geometry=True). When geometry is present,
    latitude and longitude are extracted into columns for downstream
    Haversine matching.

    Args:
        records: List of dicts from _fetch_features_paginated().

    Returns:
        DataFrame with API field columns, plus latitude/longitude when
        geometry was included. objectid is renamed to sample_id.
    """
    if not records:
        return pd.DataFrame()

    # Detect format: full feature dicts have an 'attributes' key
    if records and isinstance(records[0], dict) and 'attributes' in records[0]:
        rows = []
        for record in records:
            attrs = record.get('attributes', {})
            geom = record.get('geometry') or {}
            attrs['latitude'] = geom.get('y')
            attrs['longitude'] = geom.get('x')
            rows.append(attrs)
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(records)

    if df.empty:
        return df

    # Normalize site names
    if 'SiteName' in df.columns:
        df['SiteName'] = df['SiteName'].apply(_normalize_site_name)

    # Filter to only QAQC-complete records (defense-in-depth)
    if 'QAQC_Complete' in df.columns:
        before = len(df)
        df = df[df['QAQC_Complete'].notna()].copy()
        filtered = before - len(df)
        if filtered > 0:
            logger.warning(f"Filtered {filtered} records missing QAQC_Complete")

    # Rename objectid → sample_id
    if 'objectid' in df.columns:
        df = df.rename(columns={'objectid': 'sample_id'})

    logger.info(f"Prepared {len(df)} records for processing")
    return df
```

**Step 6: Update `format_to_database_schema` to preserve lat/lon columns**

In `format_to_database_schema` (~line 248-280), the `required_columns` list filters out any columns not listed. Add latitude/longitude passthrough:

After the line `if has_sample_id: required_columns.append('sample_id')`, add:

```python
        # Preserve geometry columns for Haversine site resolution
        if 'latitude' in formatted_df.columns:
            required_columns.append('latitude')
        if 'longitude' in formatted_df.columns:
            required_columns.append('longitude')
```

**Step 7: Mark arcgis_sync.py as resolved**

```bash
git add data_processing/arcgis_sync.py
```

---

### Task 2: Resolve merge_sites.py conflict

**Files:**
- Modify: `data_processing/merge_sites.py`

**Step 1: Take main's version, then add module-level haversine_m**

```bash
git checkout origin/main -- data_processing/merge_sites.py
```

Main's version has `load_reference_data(conn)` and `haversine_m` is nested inside `find_duplicate_coordinate_groups`. We need it at module level for `site_manager.py` to import.

**Step 2: Extract haversine_m to module level**

Add the function after the logger setup line and before `load_reference_data`:

```python
logger = setup_logging("merge_sites", category="processing")


def haversine_m(lat1, lon1, lat2, lon2):
    """Great-circle distance between two lat/lon points in meters."""
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_reference_data(conn):
```

**Step 3: Remove the nested haversine_m inside find_duplicate_coordinate_groups**

Inside `find_duplicate_coordinate_groups`, find the nested `def haversine_m(...)` function and delete it. All existing calls within the function will now use the module-level version.

**Step 4: Mark as resolved**

```bash
git add data_processing/merge_sites.py
```

---

### Task 3: Resolve updated_chemical_processing.py modify/delete conflict

**Files:**
- Delete: `data_processing/updated_chemical_processing.py`

**Step 1: Accept main's deletion**

```bash
git rm data_processing/updated_chemical_processing.py
```

**Step 2: Delete the lat/lon passthrough test that imports from it**

`tests/data_processing/test_latlon_passthrough.py` imports `format_to_database_schema` from the deleted module. This test is now obsolete — the lat/lon passthrough is handled in `arcgis_sync.py`'s `format_to_database_schema` and `prepare_dataframe` instead. Delete it:

```bash
git rm tests/data_processing/test_latlon_passthrough.py
```

---

### Task 4: Update chemical_processor.py to work with main's pipeline

**Files:**
- Modify: `cloud_functions/survey123_sync/chemical_processor.py`

**Step 1: Merge main's comprehensive version with PR #17's site resolution**

The current HEAD version is PR #17's simplified `chemical_processor.py` that delegates to `site_manager.resolve_unknown_site`. Main's version has `_resolve_site()` with exact → normalized → alias matching, plus comprehensive unknown-site tracking (which we don't need since we auto-insert).

PR #17's version is the correct one — it already:
- Imports `resolve_unknown_site` from `site_manager`
- Has proper site resolution delegation
- Tracks `new_sites_created` instead of unknown sites
- Has the correct simplified return dict

However, it's missing imports that main added (`re`, `SITE_ALIASES`, `normalize_site_name`, etc.). Since PR #17 delegates all resolution to `site_manager.py`, these imports aren't needed. The HEAD version is correct.

**No changes needed** — `chemical_processor.py` auto-merged cleanly (main.py also auto-merged). Verify by checking that `git status` shows it's not in the conflicted list.

---

### Task 5: Update main.py to use prepare_dataframe instead of translate_to_pipeline_schema

**Files:**
- Modify: `cloud_functions/survey123_sync/main.py`

**Step 1: Replace translate_to_pipeline_schema with prepare_dataframe**

In `main.py` line 300, change:

```python
            df = arcgis_sync.translate_to_pipeline_schema(records)
```

to:

```python
            df = arcgis_sync.prepare_dataframe(records)
```

`translate_to_pipeline_schema` is the old function name from the HEAD branch; main renamed it to `prepare_dataframe` and changed its internals (works with API field names directly instead of translating to CSV column names).

**Step 2: Stage main.py**

```bash
git add cloud_functions/survey123_sync/main.py
```

---

### Task 6: Update test_arcgis_geometry.py for new return_geometry parameter

**Files:**
- Modify: `tests/data_processing/test_arcgis_geometry.py`

The tests currently test the OLD `_fetch_features_paginated` behavior (always injects lat/lon into attributes, always sends `returnGeometry: 'true'`). Main's version uses a `return_geometry` parameter that changes the return format.

**Step 1: Rewrite tests for the new parameter-based approach**

```python
"""Tests for geometry extraction from FeatureServer responses."""
import unittest
from unittest.mock import patch, MagicMock
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from data_processing.arcgis_sync import _fetch_features_paginated


class TestGeometryExtraction(unittest.TestCase):
    """Verify geometry handling in _fetch_features_paginated."""

    @patch('data_processing.arcgis_sync.requests.get')
    def test_return_geometry_true_returns_full_feature_dicts(self, mock_get):
        """With return_geometry=True, records should be full feature dicts."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'features': [
                {
                    'attributes': {'objectid': 1, 'SiteName': 'Test Creek'},
                    'geometry': {'x': -97.5, 'y': 35.4},
                },
            ],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        records = _fetch_features_paginated(
            where="1=1",
            out_fields=['objectid', 'SiteName'],
            order_by_fields='objectid',
            return_geometry=True,
        )

        self.assertEqual(len(records), 1)
        # Full feature dict preserved
        self.assertIn('attributes', records[0])
        self.assertIn('geometry', records[0])
        self.assertEqual(records[0]['geometry']['x'], -97.5)
        self.assertEqual(records[0]['geometry']['y'], 35.4)

    @patch('data_processing.arcgis_sync.requests.get')
    def test_return_geometry_false_returns_attribute_dicts(self, mock_get):
        """With return_geometry=False (default), records are flat attribute dicts."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'features': [
                {
                    'attributes': {'objectid': 2, 'SiteName': 'No Geo Creek'},
                },
            ],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        records = _fetch_features_paginated(
            where="1=1",
            out_fields=['objectid', 'SiteName'],
            order_by_fields='objectid',
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['objectid'], 2)
        self.assertNotIn('attributes', records[0])

    @patch('data_processing.arcgis_sync.requests.get')
    def test_return_geometry_param_sent_in_request(self, mock_get):
        """The returnGeometry param should match the argument."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'features': []}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        _fetch_features_paginated(
            where="1=1",
            out_fields=['objectid'],
            order_by_fields='objectid',
            return_geometry=True,
        )

        call_args = mock_get.call_args
        params = call_args.kwargs.get('params') or call_args[1].get('params')
        self.assertEqual(params.get('returnGeometry'), True)


if __name__ == '__main__':
    unittest.main()
```

**Step 2: Stage**

```bash
git add tests/data_processing/test_arcgis_geometry.py
```

---

### Task 7: Add test for prepare_dataframe geometry extraction

**Files:**
- Create: `tests/data_processing/test_prepare_dataframe_geometry.py`

This replaces the deleted `test_latlon_passthrough.py` — tests that `prepare_dataframe` correctly extracts lat/lon from full feature dicts into flat DataFrame columns.

**Step 1: Write the test**

```python
"""Tests that prepare_dataframe extracts geometry into lat/lon columns."""
import unittest
import os
import sys

import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from data_processing.arcgis_sync import prepare_dataframe


class TestPrepareDataframeGeometry(unittest.TestCase):
    """Verify lat/lon extraction in prepare_dataframe."""

    def test_geometry_extracted_to_columns(self):
        """Full feature dicts should get latitude/longitude columns."""
        records = [
            {
                'attributes': {
                    'objectid': 1,
                    'SiteName': 'Test Creek',
                    'QAQC_Complete': 'Yes',
                },
                'geometry': {'x': -97.5, 'y': 35.4},
            },
        ]
        df = prepare_dataframe(records)
        self.assertIn('latitude', df.columns)
        self.assertIn('longitude', df.columns)
        self.assertAlmostEqual(df.iloc[0]['latitude'], 35.4)
        self.assertAlmostEqual(df.iloc[0]['longitude'], -97.5)

    def test_missing_geometry_gives_none(self):
        """Records without geometry should have None for lat/lon."""
        records = [
            {
                'attributes': {
                    'objectid': 2,
                    'SiteName': 'No Geo Creek',
                    'QAQC_Complete': 'Yes',
                },
            },
        ]
        df = prepare_dataframe(records)
        self.assertIn('latitude', df.columns)
        self.assertIsNone(df.iloc[0]['latitude'])

    def test_flat_attribute_dicts_still_work(self):
        """Attribute-only dicts (no 'attributes' key) should still parse."""
        records = [
            {
                'objectid': 3,
                'SiteName': 'Flat Creek',
                'QAQC_Complete': 'Yes',
            },
        ]
        df = prepare_dataframe(records)
        self.assertIn('sample_id', df.columns)
        self.assertEqual(df.iloc[0]['sample_id'], 3)

    def test_objectid_renamed_to_sample_id(self):
        """objectid column should be renamed to sample_id."""
        records = [
            {
                'attributes': {
                    'objectid': 10,
                    'SiteName': 'Rename Creek',
                    'QAQC_Complete': 'Yes',
                },
                'geometry': {'x': -97.0, 'y': 35.0},
            },
        ]
        df = prepare_dataframe(records)
        self.assertIn('sample_id', df.columns)
        self.assertNotIn('objectid', df.columns)


if __name__ == '__main__':
    unittest.main()
```

**Step 2: Stage**

```bash
git add tests/data_processing/test_prepare_dataframe_geometry.py
```

---

### Task 8: Add test for format_to_database_schema lat/lon passthrough

**Files:**
- Create: `tests/data_processing/test_format_latlon_passthrough.py`

Replaces the deleted test but tests the `arcgis_sync.py` version of `format_to_database_schema`.

**Step 1: Write the test**

```python
"""Tests that lat/lon columns survive format_to_database_schema."""
import unittest
import os
import sys

import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from data_processing.arcgis_sync import format_to_database_schema


class TestFormatLatLonPassthrough(unittest.TestCase):
    """Verify latitude/longitude columns are preserved through formatting."""

    def _make_sample_df(self, include_coords=True):
        """Create a minimal DataFrame matching arcgis_sync's API-first schema."""
        data = {
            'SiteName': ['Test Creek'],
            'day': [pd.Timestamp('2026-01-15')],
            'Year': [2026],
            'Month': [1],
            'do_percent': [95.0],
            'pH': [7.2],
            'Nitrate': [1.0],
            'Nitrite': [0.1],
            'Ammonia': [0.5],
            'Phosphorus': [0.05],
            'Chloride': [25.0],
            'soluble_nitrogen': [1.6],
            'sample_id': [123],
        }
        if include_coords:
            data['latitude'] = [35.4]
            data['longitude'] = [-97.5]
        return pd.DataFrame(data)

    def test_latlon_preserved_when_present(self):
        """latitude and longitude should survive format_to_database_schema."""
        df = self._make_sample_df(include_coords=True)
        result = format_to_database_schema(df)
        self.assertIn('latitude', result.columns)
        self.assertIn('longitude', result.columns)
        self.assertAlmostEqual(result.iloc[0]['latitude'], 35.4)
        self.assertAlmostEqual(result.iloc[0]['longitude'], -97.5)

    def test_no_latlon_still_works(self):
        """Pipeline should still work when lat/lon columns are absent."""
        df = self._make_sample_df(include_coords=False)
        result = format_to_database_schema(df)
        self.assertNotIn('latitude', result.columns)
        self.assertNotIn('longitude', result.columns)
        self.assertGreater(len(result), 0)


if __name__ == '__main__':
    unittest.main()
```

**Step 2: Stage**

```bash
git add tests/data_processing/test_format_latlon_passthrough.py
```

---

### Task 9: Run tests and commit

**Step 1: Run the full test suite**

```bash
pytest -x -q 2>&1 | tail -20
```

Expected: All tests pass (including the new geometry tests and the existing site_manager tests).

**Step 2: If tests fail, diagnose and fix**

Common issues:
- `translate_to_pipeline_schema` still referenced somewhere → grep and update
- Import errors from deleted `updated_chemical_processing` → grep and update
- `format_to_database_schema` test needs different column names (API-first uses different names than CSV pipeline)

**Step 3: Stage all remaining changes and commit**

```bash
git add -A
git commit -m "merge: resolve conflicts with main (PR #16 API-first pipeline)

- Take main's arcgis_sync.py with return_geometry parameter approach
- Pass return_geometry=True from sync fetch calls (enables Haversine dedup)
- Update prepare_dataframe to extract lat/lon from full feature dicts
- Update format_to_database_schema to preserve lat/lon columns
- Extract haversine_m to module level in merge_sites.py (for site_manager import)
- Delete updated_chemical_processing.py (replaced by Feature Server pipeline)
- Replace translate_to_pipeline_schema with prepare_dataframe in main.py
- Update geometry tests for new return_geometry parameter approach"
```

**Step 4: Verify merge commit is clean**

```bash
git diff origin/main...HEAD --stat
pytest -x -q
```
