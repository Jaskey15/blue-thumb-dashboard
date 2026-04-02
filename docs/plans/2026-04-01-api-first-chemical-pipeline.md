# API-First Chemical Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the CSV-based `updated_chemical_processing.py` with an API-first pipeline that fetches chemical data directly from the ArcGIS Feature Server, eliminating the double field-name translation and stale CSV dependency.

**Architecture:** Processing functions move into `arcgis_sync.py`, rewritten to use API field names directly. Site consolidation swaps its priority 4 CSV source for a Feature Server fetch. `reset_database.py` calls `arcgis_sync.sync_all_chemical_data()` instead of `load_updated_chemical_data_to_db()`.

**Tech Stack:** Python, pandas, requests, SQLite, pytest (with mocked HTTP)

**Design doc:** `docs/plans/2026-04-01-api-first-chemical-pipeline-design.md`

---

### Task 1: Rewrite processing functions in arcgis_sync.py to use API field names

**Files:**
- Modify: `data_processing/arcgis_sync.py:26-50` (new NUTRIENT_COLUMN_MAPPINGS), `:61-96` (replace ARCGIS_FIELD_MAP), `:228-319` (replace translate_to_pipeline_schema and process_fetched_data)

This is the core change. Replace the CSV-translation approach with direct API-field-name processing.

**Step 1: Replace ARCGIS_FIELD_MAP and imports**

Remove the import block at lines 40-45:
```python
from data_processing.updated_chemical_processing import (
    format_to_database_schema,
    parse_sampling_dates,
    process_conditional_nutrient,
    process_simple_nutrients,
)
```

Replace `ARCGIS_FIELD_MAP` (lines 61-96) with a new `NUTRIENT_COLUMN_MAPPINGS` dict using API field names and a minimal `COLUMN_TO_DB` rename map:

```python
# Maps API field names directly to DB column names (only fields that need renaming).
COLUMN_TO_DB = {
    'SiteName': 'Site_Name',
    'oxygen_sat': 'do_percent',
    'Orthophosphate': 'Phosphorus',
}

# Fields to request from Feature Server for chemical processing.
CHEMICAL_FIELDS = [
    'objectid', 'SiteName', 'day', 'oxygen_sat',
    'pH1', 'pH2', 'nitratetest1', 'nitratetest2',
    'nitritetest1', 'nitritetest2',
    'Ammonia_Range', 'ammonia_Nitrogen2', 'ammonia_Nitrogen3',
    'Ammonia_nitrogen_midrange1_Final', 'Ammonia_nitrogen_midrange2_Final',
    'Ortho_Range', 'Orthophosphate_Low1_Final', 'Orthophosphate_Low2_Final',
    'Orthophosphate_Mid1_Final', 'Orthophosphate_Mid2_Final',
    'Orthophosphate_High1_Final', 'Orthophosphate_High2_Final',
    'Chloride_Range', 'Chloride_Low1_Final', 'Chloride_Low2_Final',
    'Chloride_High1_Final', 'Chloride_High2_Final',
    'QAQC_Complete',
]

# Nutrient column mappings using API field names directly.
NUTRIENT_COLUMN_MAPPINGS = {
    'ammonia': {
        'range_selection': 'Ammonia_Range',
        'low_col1': 'ammonia_Nitrogen2',
        'low_col2': 'ammonia_Nitrogen3',
        'mid_col1': 'Ammonia_nitrogen_midrange1_Final',
        'mid_col2': 'Ammonia_nitrogen_midrange2_Final',
    },
    'orthophosphate': {
        'range_selection': 'Ortho_Range',
        'low_col1': 'Orthophosphate_Low1_Final',
        'low_col2': 'Orthophosphate_Low2_Final',
        'mid_col1': 'Orthophosphate_Mid1_Final',
        'mid_col2': 'Orthophosphate_Mid2_Final',
        'high_col1': 'Orthophosphate_High1_Final',
        'high_col2': 'Orthophosphate_High2_Final',
    },
    'chloride': {
        'range_selection': 'Chloride_Range',
        'low_col1': 'Chloride_Low1_Final',
        'low_col2': 'Chloride_Low2_Final',
        'high_col1': 'Chloride_High1_Final',
        'high_col2': 'Chloride_High2_Final',
    },
}
```

Update `OUT_FIELDS = CHEMICAL_FIELDS` (replaces the old `OUT_FIELDS = list(ARCGIS_FIELD_MAP.keys())`).

**Step 2: Add processing functions (moved from updated_chemical_processing.py)**

Add these functions to `arcgis_sync.py`, rewritten to use API field names:

- `get_greater_value(row, col1, col2)` — unchanged logic, generic utility
- `get_ph_worst_case(row)` — uses `'pH1'`, `'pH2'` instead of `'pH #1'`, `'pH #2'`
- `get_conditional_nutrient_value(row, ...)` — unchanged logic, generic utility
- `process_conditional_nutrient(df, nutrient_name)` — uses the new `NUTRIENT_COLUMN_MAPPINGS`
- `process_simple_nutrients(df)` — uses `'nitratetest1'`, `'nitratetest2'`, `'nitritetest1'`, `'nitritetest2'`
- `parse_epoch_dates(df)` — NEW: converts `'day'` epoch ms directly to `Date`, `Year`, `Month` columns using Central timezone. No string round-trip.
- `format_to_database_schema(df)` — uses `COLUMN_TO_DB` for renaming, adds pH worst-case calc and soluble nitrogen

**Step 3: Rewrite translate_to_pipeline_schema → prepare_dataframe**

Replace `translate_to_pipeline_schema()` (lines 228-286) with a simpler `prepare_dataframe()` that:
1. Creates DataFrame from raw API records
2. Normalizes site names (keep `_normalize_site_name`)
3. Filters QAQC-complete records (defense-in-depth)
4. Renames `objectid` → `sample_id`

No field-name translation, no epoch→string→date conversion. The DataFrame keeps API field names.

**Step 4: Rewrite process_fetched_data**

Replace `process_fetched_data()` (lines 289-319) to call the new local functions instead of imported ones:

```python
def process_fetched_data(df):
    if df.empty:
        return pd.DataFrame()

    df = parse_epoch_dates(df)
    df = process_simple_nutrients(df)
    df['Ammonia'] = process_conditional_nutrient(df, 'ammonia')
    df['Orthophosphate'] = process_conditional_nutrient(df, 'orthophosphate')
    df['Chloride'] = process_conditional_nutrient(df, 'chloride')

    formatted_df = format_to_database_schema(df)
    formatted_df = remove_empty_chemical_rows(formatted_df)
    formatted_df = validate_chemical_data(formatted_df, remove_invalid=True)
    formatted_df = apply_bdl_conversions(formatted_df)

    return formatted_df
```

**Step 5: Update fetch_features_since to use CHEMICAL_FIELDS**

Update `fetch_features_since()` (line 129) and `fetch_features_edited_since()` (line 150) to use `CHEMICAL_FIELDS` instead of `OUT_FIELDS` if `OUT_FIELDS` was renamed. (If `OUT_FIELDS = CHEMICAL_FIELDS`, no change needed.)

**Step 6: Run existing tests to check nothing else broke**

Run: `pytest tests/ -v --tb=short`
Expected: Some tests will fail due to removed imports from `updated_chemical_processing`. That's expected — we fix tests in Task 4.

**Step 7: Commit**

```bash
git add data_processing/arcgis_sync.py
git commit -m "refactor: rewrite arcgis_sync processing to use API field names directly

Replace ARCGIS_FIELD_MAP CSV translation with direct API-to-DB processing.
Move nutrient selection, pH worst-case, and date parsing logic into
arcgis_sync.py, eliminating dependency on updated_chemical_processing.py."
```

---

### Task 2: Add sync_all_chemical_data entry point

**Files:**
- Modify: `data_processing/arcgis_sync.py` (add new function after `sync_new_chemical_data`)

**Step 1: Write the function**

Add `sync_all_chemical_data()` after `sync_new_chemical_data()`. This is the full-fetch entry point for `reset_database.py`:

```python
def sync_all_chemical_data(dry_run=False):
    """
    Fetch ALL current-period chemical records from Feature Server and insert into DB.

    Used by reset_database.py for full database rebuilds. Unlike sync_new_chemical_data()
    which fetches incrementally by date, this fetches everything.

    Args:
        dry_run: If True, fetch and process but skip database insertion.

    Returns:
        Dictionary with sync results and statistics.
    """
    start_time = datetime.now()
    logger.info("=== ArcGIS Full Sync: fetching ALL records ===")

    records = _fetch_features_paginated(
        where="QAQC_Complete IS NOT NULL",
        out_fields=CHEMICAL_FIELDS,
        order_by_fields='day ASC',
    )

    if not records:
        logger.info("No records found on Feature Server")
        return {
            'status': 'success',
            'records_fetched': 0,
            'records_inserted': 0,
            'execution_time': str(datetime.now() - start_time),
        }

    df = prepare_dataframe(records)
    if df.empty:
        return {
            'status': 'success',
            'records_fetched': len(records),
            'records_after_qaqc': 0,
            'records_inserted': 0,
            'execution_time': str(datetime.now() - start_time),
        }

    processed_df = process_fetched_data(df)
    if processed_df.empty:
        return {
            'status': 'success',
            'records_fetched': len(records),
            'records_after_processing': 0,
            'records_inserted': 0,
            'execution_time': str(datetime.now() - start_time),
        }

    filtered_df, skipped_sites = filter_known_sites(processed_df)

    if dry_run:
        logger.info(f"DRY RUN: would insert {len(filtered_df)} records")
        return {
            'status': 'dry_run',
            'records_fetched': len(records),
            'records_after_processing': len(processed_df),
            'records_ready': len(filtered_df),
            'skipped_sites': skipped_sites,
            'execution_time': str(datetime.now() - start_time),
        }

    if filtered_df.empty:
        return {
            'status': 'success',
            'records_fetched': len(records),
            'records_inserted': 0,
            'skipped_sites': skipped_sites,
            'execution_time': str(datetime.now() - start_time),
        }

    stats = insert_chemical_data(filtered_df, data_source="arcgis_feature_server")

    result = {
        'status': 'success',
        'records_fetched': len(records),
        'records_after_processing': len(processed_df),
        'records_inserted': stats.get('measurements_added', 0),
        'events_added': stats.get('events_added', 0),
        'sites_processed': stats.get('sites_processed', 0),
        'skipped_sites': skipped_sites,
        'execution_time': str(datetime.now() - start_time),
    }

    logger.info(f"=== Full sync complete: {result['records_inserted']} measurements inserted ===")
    return result
```

**Step 2: Commit**

```bash
git add data_processing/arcgis_sync.py
git commit -m "feat: add sync_all_chemical_data for full Feature Server fetch

New entry point for reset_database.py that fetches all QAQC-complete
records without a date filter, for full database rebuilds."
```

---

### Task 3: Add extract_sites_from_feature_server to consolidate_sites.py

**Files:**
- Modify: `data_processing/consolidate_sites.py:29-89` (CSV_CONFIGS), `:92-170` (clean_all_csvs), `:251-338` (consolidate_sites), `:376-413` (verify_cleaned_csvs)
- Modify: `data_processing/arcgis_sync.py` (add site extraction function)

**Step 1: Add fetch_site_data function to arcgis_sync.py**

Add a function that fetches distinct sites with geometry and county from the Feature Server:

```python
def fetch_site_data(timeout_seconds=30):
    """
    Fetch distinct site names, coordinates, and county from the Feature Server.

    Used by consolidate_sites.py to register Feature Server sites during
    site consolidation (priority 4 slot).

    Returns:
        DataFrame with columns: site_name, latitude, longitude, county,
        river_basin, ecoregion, source_file, source_description
    """
    records = _fetch_features_paginated(
        where="1=1",
        out_fields=['SiteName', 'CountyName'],
        order_by_fields='SiteName ASC',
        timeout_seconds=timeout_seconds,
        return_geometry=True,
    )

    if not records:
        logger.warning("No sites found on Feature Server")
        return pd.DataFrame()

    rows = []
    for record in records:
        attrs = record if isinstance(record, dict) and 'geometry' not in record else record.get('attributes', record)
        geom = record.get('geometry', {})
        rows.append({
            'site_name': _normalize_site_name(attrs.get('SiteName')),
            'latitude': geom.get('y'),
            'longitude': geom.get('x'),
            'county': attrs.get('CountyName'),
            'river_basin': None,
            'ecoregion': None,
            'source_file': 'arcgis_feature_server',
            'source_description': 'ArcGIS Feature Server',
        })

    df = pd.DataFrame(rows)
    df = df[df['site_name'].notna() & (df['site_name'] != '')]

    # Deduplicate by site name, warn if coordinates differ
    before = len(df)
    df = df.drop_duplicates(subset=['site_name'], keep='first')
    dupes = before - len(df)
    if dupes > 0:
        logger.info(f"Deduplicated {dupes} duplicate site entries")

    logger.info(f"Fetched {len(df)} unique sites from Feature Server")
    return df
```

**Note:** This requires `_fetch_features_paginated` to support `return_geometry`. Check if PR #15 already added this — if so, use it. If not, add a `return_geometry=False` parameter to `_fetch_features_paginated` that adds `'returnGeometry': return_geometry` to the params dict. When `return_geometry=True`, the function should return the full feature dicts (not just attributes) so geometry is accessible.

**Step 2: Modify consolidate_sites.py**

Remove `cleaned_updated_chemical_data.csv` from `CSV_CONFIGS` (delete lines 61-69).

Remove `'updated_chemical_data.csv'` from the `clean_all_csvs()` file list (line 108).

Remove special-case handling for `updated_chemical_data.csv` in `clean_all_csvs()` (lines 121-123).

Remove `cleaned_updated_chemical_data.csv` from `verify_cleaned_csvs()` — this happens automatically since it checks `CSV_CONFIGS`.

Modify `consolidate_sites()` to handle the Feature Server source. Change the loop at line 270:

```python
def consolidate_sites():
    # ... existing setup ...

    for i, config in enumerate(CSV_CONFIGS):
        logger.info(f"\nProcessing priority {i+1}: {config['description']}")

        csv_sites = extract_sites_from_csv(config)

        # ... rest of loop unchanged ...
```

To insert the Feature Server fetch at the right priority position. The simplest approach: after processing priority 3 (fish_data), call `fetch_site_data()` before continuing with the remaining CSV configs. This can be done by splitting CSV_CONFIGS into two lists (priorities 1-3 and 5-6) and inserting the API call between them, OR by adding a special marker in CSV_CONFIGS.

Recommended approach — keep it simple with a dedicated call:

```python
# In consolidate_sites(), after the CSV_CONFIGS loop processes priorities 1-3:
# Insert Feature Server sites at priority 4
from data_processing.arcgis_sync import fetch_site_data

# Process CSV sources (priorities 1-3 are configs[0:3], priorities 5-6 are configs[3:5])
# Feature Server is priority 4, called between them
```

Split `CSV_CONFIGS` into `CSV_CONFIGS_HIGH` (site_data, chemical_data, fish_data) and `CSV_CONFIGS_LOW` (macro_data, habitat_data). Process high-priority CSVs, then Feature Server, then low-priority CSVs using the same merge logic.

**Step 3: Commit**

```bash
git add data_processing/arcgis_sync.py data_processing/consolidate_sites.py
git commit -m "feat: replace updated_chemical_data CSV with Feature Server in site consolidation

Priority 4 slot now fetches sites from the ArcGIS Feature Server
(site name, coordinates, county) instead of reading a CSV.
Site consolidation logic and priority ordering unchanged."
```

---

### Task 4: Update reset_database.py and remove updated_chemical_processing.py

**Files:**
- Modify: `database/reset_database.py:15` (import), `:165` (function call)
- Modify: `data_processing/data_loader.py:32` (remove updated_chemical reference)
- Delete: `data_processing/updated_chemical_processing.py`

**Step 1: Update reset_database.py**

Replace import at line 15:
```python
# Before
from data_processing.updated_chemical_processing import load_updated_chemical_data_to_db

# After
from data_processing.arcgis_sync import sync_all_chemical_data
```

Replace call at line 165:
```python
# Before
updated_result = load_updated_chemical_data_to_db()

# After
updated_result = sync_all_chemical_data()
```

Adjust any result-checking logic around line 165 to handle the dict return value from `sync_all_chemical_data()` instead of the boolean from `load_updated_chemical_data_to_db()`. The existing function returns `True`/`False`; the new one returns a dict with `'status': 'success'` or raises. Update the success check accordingly.

**Step 2: Update data_loader.py**

Remove line 32:
```python
'updated_chemical': os.path.join(INTERIM_DATA_DIR, 'cleaned_updated_chemical_data.csv'),
```

**Step 3: Delete updated_chemical_processing.py**

```bash
git rm data_processing/updated_chemical_processing.py
```

**Step 4: Run the test suite (expect some failures from test imports)**

Run: `pytest tests/ -v --tb=short`
Expected: Tests that imported from `updated_chemical_processing` will fail. Fix in Task 5.

**Step 5: Commit**

```bash
git add database/reset_database.py data_processing/data_loader.py
git rm data_processing/updated_chemical_processing.py
git commit -m "feat: wire reset_database to use Feature Server, retire updated_chemical_processing

reset_database.py Phase 2 now calls sync_all_chemical_data() from
arcgis_sync.py. Removes updated_chemical_processing.py entirely."
```

---

### Task 5: Update and write tests

**Files:**
- Modify: `tests/data_processing/test_chemical_processing.py` (remove updated_chemical_processing imports, update test data)
- Create: `tests/data_processing/test_arcgis_sync.py`

**Step 1: Clean up test_chemical_processing.py**

Remove imports from `updated_chemical_processing` (lines 29-38).
Remove import of `translate_to_pipeline_schema` from `arcgis_sync` (line 39).
Update imports to pull moved functions from `arcgis_sync`:

```python
from data_processing.arcgis_sync import (
    format_to_database_schema,
    get_conditional_nutrient_value,
    get_greater_value,
    get_ph_worst_case,
    parse_epoch_dates,
    process_conditional_nutrient,
    process_simple_nutrients,
)
```

Update tests that use CSV column names to use API field names:

- `test_parse_sampling_dates` (line 231) → rewrite as `test_parse_epoch_dates` using epoch ms input
- `test_translate_to_pipeline_schema_normalizes_site_and_sets_sample_id` (line 259) → rewrite as `test_prepare_dataframe` using API field names
- `test_format_to_database_schema_preserves_sample_id` (line 305) → update column names to API names
- `test_get_ph_worst_case` (line 390) → use `'pH1'`, `'pH2'` instead of `'pH #1'`, `'pH #2'`
- `test_process_simple_nutrients` (line 512) → use `'nitratetest1'`, `'nitratetest2'`, etc.
- `test_format_to_database_schema` (line 530) → update input column names
- `test_updated_processing_pipeline` (line 622) → delete (CSV pipeline no longer exists)

Tests that are generic and don't reference column names stay unchanged:
- `test_get_greater_value` (line 416)
- `test_get_conditional_nutrient_value` (line 444)
- `test_process_conditional_nutrient` (line 493) — may need column name updates in test data

**Step 2: Create test_arcgis_sync.py with new tests**

```python
"""Tests for the API-first chemical data pipeline in arcgis_sync.py."""

import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock

import pandas as pd

from data_processing.arcgis_sync import (
    fetch_site_data,
    prepare_dataframe,
    parse_epoch_dates,
    process_fetched_data,
    sync_all_chemical_data,
    _fetch_features_paginated,
)


class TestPrepareDataframe(unittest.TestCase):
    """Tests for prepare_dataframe (replaces translate_to_pipeline_schema)."""

    def test_normalizes_site_names(self):
        records = [{'SiteName': 'Coffee Creek:  N. Sooner Rd.', 'objectid': 1, 'QAQC_Complete': 'X'}]
        df = prepare_dataframe(records)
        self.assertEqual(df['SiteName'].iloc[0], 'Coffee Creek: N. Sooner Rd.')

    def test_renames_objectid_to_sample_id(self):
        records = [{'SiteName': 'Test', 'objectid': 42, 'QAQC_Complete': 'X'}]
        df = prepare_dataframe(records)
        self.assertEqual(df['sample_id'].iloc[0], 42)

    def test_filters_qaqc_incomplete(self):
        records = [
            {'SiteName': 'A', 'objectid': 1, 'QAQC_Complete': 'X'},
            {'SiteName': 'B', 'objectid': 2, 'QAQC_Complete': None},
        ]
        df = prepare_dataframe(records)
        self.assertEqual(len(df), 1)

    def test_empty_records(self):
        df = prepare_dataframe([])
        self.assertTrue(df.empty)


class TestParseEpochDates(unittest.TestCase):
    """Tests for direct epoch ms → date conversion."""

    def test_converts_epoch_to_date(self):
        # 2025-03-15 in epoch ms (UTC)
        epoch_ms = 1742025600000
        df = pd.DataFrame({'day': [epoch_ms]})
        result = parse_epoch_dates(df)
        self.assertIn('Date', result.columns)
        self.assertIn('Year', result.columns)
        self.assertIn('Month', result.columns)
        self.assertEqual(result['Year'].iloc[0], 2025)
        self.assertEqual(result['Month'].iloc[0], 3)

    def test_handles_null_dates(self):
        df = pd.DataFrame({'day': [None]})
        result = parse_epoch_dates(df)
        self.assertTrue(pd.isna(result['Date'].iloc[0]))


class TestFetchSiteData(unittest.TestCase):
    """Tests for Feature Server site extraction."""

    @patch('data_processing.arcgis_sync._fetch_features_paginated')
    def test_extracts_sites_with_geometry(self, mock_fetch):
        mock_fetch.return_value = [
            {
                'attributes': {'SiteName': 'Wolf Creek: Gore Blvd.', 'CountyName': 'Comanche'},
                'geometry': {'x': -98.44398, 'y': 34.60876},
            },
            {
                'attributes': {'SiteName': 'Coal Creek: Hwy 11', 'CountyName': 'Tulsa'},
                'geometry': {'x': -95.914999, 'y': 36.195556},
            },
        ]
        df = fetch_site_data()
        self.assertEqual(len(df), 2)
        self.assertAlmostEqual(df.iloc[0]['latitude'], 34.60876)
        self.assertAlmostEqual(df.iloc[0]['longitude'], -98.44398)
        self.assertEqual(df.iloc[0]['county'], 'Comanche')

    @patch('data_processing.arcgis_sync._fetch_features_paginated')
    def test_deduplicates_by_site_name(self, mock_fetch):
        mock_fetch.return_value = [
            {
                'attributes': {'SiteName': 'Same Site', 'CountyName': 'Tulsa'},
                'geometry': {'x': -95.9, 'y': 36.2},
            },
            {
                'attributes': {'SiteName': 'Same Site', 'CountyName': 'Tulsa'},
                'geometry': {'x': -95.9, 'y': 36.2},
            },
        ]
        df = fetch_site_data()
        self.assertEqual(len(df), 1)

    @patch('data_processing.arcgis_sync._fetch_features_paginated')
    def test_empty_response(self, mock_fetch):
        mock_fetch.return_value = []
        df = fetch_site_data()
        self.assertTrue(df.empty)


class TestPagination(unittest.TestCase):
    """Tests for Feature Server pagination."""

    @patch('data_processing.arcgis_sync.requests.get')
    def test_paginates_on_exceeded_transfer_limit(self, mock_get):
        page1 = MagicMock()
        page1.json.return_value = {
            'features': [{'attributes': {'objectid': i}} for i in range(2000)],
            'exceededTransferLimit': True,
        }
        page1.raise_for_status = MagicMock()

        page2 = MagicMock()
        page2.json.return_value = {
            'features': [{'attributes': {'objectid': i}} for i in range(2000, 2500)],
        }
        page2.raise_for_status = MagicMock()

        mock_get.side_effect = [page1, page2]

        records = _fetch_features_paginated(
            where="1=1", out_fields=['objectid'], order_by_fields='objectid ASC'
        )
        self.assertEqual(len(records), 2500)


class TestSyncAllChemicalData(unittest.TestCase):
    """Tests for the full-fetch entry point."""

    @patch('data_processing.arcgis_sync.insert_chemical_data')
    @patch('data_processing.arcgis_sync.filter_known_sites')
    @patch('data_processing.arcgis_sync.process_fetched_data')
    @patch('data_processing.arcgis_sync.prepare_dataframe')
    @patch('data_processing.arcgis_sync._fetch_features_paginated')
    def test_full_sync_pipeline(self, mock_fetch, mock_prepare, mock_process, mock_filter, mock_insert):
        mock_fetch.return_value = [{'objectid': 1}]
        mock_prepare.return_value = pd.DataFrame({'Site_Name': ['Test'], 'Date': ['2025-01-01']})
        mock_process.return_value = pd.DataFrame({'Site_Name': ['Test'], 'Date': ['2025-01-01']})
        mock_filter.return_value = (pd.DataFrame({'Site_Name': ['Test'], 'Date': ['2025-01-01']}), [])
        mock_insert.return_value = {'measurements_added': 1, 'events_added': 1, 'sites_processed': 1}

        result = sync_all_chemical_data()
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['records_inserted'], 1)
        mock_fetch.assert_called_once()

    @patch('data_processing.arcgis_sync._fetch_features_paginated')
    def test_empty_fetch_returns_zero(self, mock_fetch):
        mock_fetch.return_value = []
        result = sync_all_chemical_data()
        self.assertEqual(result['records_fetched'], 0)
        self.assertEqual(result['records_inserted'], 0)


class TestProcessFetchedDataIntegration(unittest.TestCase):
    """Integration test: API-format records → DB-ready DataFrame."""

    def test_end_to_end_processing(self):
        """Verify a realistic API record processes to correct DB columns."""
        records = [{
            'objectid': 3857,
            'SiteName': 'Fisher Creek: Hwy 51',
            'day': 1742025600000,  # 2025-03-15
            'oxygen_sat': 95.0,
            'pH1': 7.8,
            'pH2': 7.2,
            'nitratetest1': 1.5,
            'nitratetest2': 1.2,
            'nitritetest1': 0.05,
            'nitritetest2': 0.03,
            'Ammonia_Range': 'Low Range',
            'ammonia_Nitrogen2': 0.1,
            'ammonia_Nitrogen3': 0.08,
            'Ammonia_nitrogen_midrange1_Final': None,
            'Ammonia_nitrogen_midrange2_Final': None,
            'Ortho_Range': 'Low Range',
            'Orthophosphate_Low1_Final': 0.02,
            'Orthophosphate_Low2_Final': 0.01,
            'Orthophosphate_Mid1_Final': None,
            'Orthophosphate_Mid2_Final': None,
            'Orthophosphate_High1_Final': None,
            'Orthophosphate_High2_Final': None,
            'Chloride_Range': 'Low Range',
            'Chloride_Low1_Final': 15.0,
            'Chloride_Low2_Final': 14.0,
            'Chloride_High1_Final': None,
            'Chloride_High2_Final': None,
            'QAQC_Complete': 'X',
        }]

        df = prepare_dataframe(records)
        result = process_fetched_data(df)

        self.assertEqual(len(result), 1)
        row = result.iloc[0]
        self.assertEqual(row['Site_Name'], 'Fisher Creek: Hwy 51')
        self.assertEqual(row['do_percent'], 95.0)
        # pH worst-case: 7.2 is further from 7 than 7.8
        self.assertEqual(row['pH'], 7.2)
        self.assertEqual(row['Nitrate'], 1.5)  # greater of 1.5, 1.2
        self.assertEqual(row['Nitrite'], 0.05)  # greater of 0.05, 0.03
        self.assertEqual(row['Ammonia'], 0.1)  # low range, greater of 0.1, 0.08
        self.assertEqual(row['Phosphorus'], 0.02)  # low range, greater of 0.02, 0.01
        self.assertEqual(row['Chloride'], 15.0)  # low range, greater of 15, 14
        self.assertIn('soluble_nitrogen', result.columns)
        self.assertIn('sample_id', result.columns)
```

**Step 3: Run all tests**

Run: `pytest tests/ -v`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add tests/data_processing/test_arcgis_sync.py tests/data_processing/test_chemical_processing.py
git commit -m "test: add arcgis_sync tests and update chemical processing tests

New test file for API-first pipeline covering site extraction, date
conversion, pagination, full sync, and end-to-end processing.
Update existing tests to import from arcgis_sync instead of
updated_chemical_processing and use API field names."
```

---

### Task 6: Update documentation

**Files:**
- Modify: `CLAUDE.md` (update chemical pathway description)
- Modify: `docs/architecture/DATA_PIPELINE.md` (update pipeline docs)

**Step 1: Update CLAUDE.md**

In the "Three chemical data pathways" gotcha, update to reflect two pathways:

```
- **Two chemical data pathways**: `chemical_processing.py` (legacy single-value CSV for pre-2020 data) and `arcgis_sync.py` (API-first pipeline fetching directly from the ArcGIS Feature Server for current-period data). Both share `chemical_utils.py`.
```

Update the Common Task Routing table entry for "Update cloud sync logic" to remove `chemical_processor.py` reference if needed.

**Step 2: Update DATA_PIPELINE.md**

Update the "Real-Time Data Ingestion" section and "Three Chemical Data Pathways" to reflect:
- `arcgis_sync.py` is now the primary pathway for all current-period data (not just real-time sync)
- `updated_chemical_processing.py` has been retired
- The processing pipeline works directly with API field names

Update the File Roles table to remove `updated_chemical_processing.py` and update `arcgis_sync.py` description.

**Step 3: Commit**

```bash
git add CLAUDE.md docs/architecture/DATA_PIPELINE.md
git commit -m "docs: update documentation for API-first chemical pipeline

Reflect retirement of updated_chemical_processing.py and the
simplified two-pathway architecture (legacy CSV + Feature Server API)."
```

---

### Task 7: Verification — full reset dry run

**Step 1: Run full test suite**

Run: `pytest tests/ -v`
Expected: All tests pass.

**Step 2: Run a local database reset (if practical)**

Run: `python -m database.reset_database`
Expected: Phase 1 site consolidation fetches sites from Feature Server at priority 4. Phase 2 loads legacy chemical data from CSV, then fetches all current-period data from Feature Server. All phases complete without errors.

If not practical to run a full reset (requires all CSVs in data/raw/), verify the individual components:

```python
# Test Feature Server connectivity and data
python -c "from data_processing.arcgis_sync import sync_all_chemical_data; print(sync_all_chemical_data(dry_run=True))"
```

**Step 3: Compare record counts**

Verify the Feature Server returns >= 3,271 records (the count verified on 2026-04-01).

**Step 4: Final commit if any fixes needed**

---

## Task Dependency Graph

```
Task 1 (rewrite processing) → Task 2 (sync_all entry point) → Task 4 (wire reset_database)
Task 1 → Task 3 (site consolidation) → Task 4
Task 1-4 → Task 5 (tests)
Task 5 → Task 6 (docs)
Task 6 → Task 7 (verification)
```

Tasks 2 and 3 can run in parallel after Task 1.
