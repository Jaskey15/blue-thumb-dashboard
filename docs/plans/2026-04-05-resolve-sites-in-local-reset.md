# Resolve Unknown Sites in Local Reset Pipeline

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace `filter_known_sites()` in `arcgis_sync.py` so `sync_all_chemical_data()` resolves unknown sites (normalize → alias → haversine → auto-insert) instead of silently dropping them. This fixes the 33 dropped sites during `reset_database`.

**Architecture:** Copy `resolve_unknown_site()` logic from `cloud_functions/survey123_sync/site_manager.py` directly into `arcgis_sync.py`. Add a new `resolve_unknown_sites()` function that applies it to a DataFrame of unknown site names. Replace the `filter_known_sites()` call in `sync_all_chemical_data()`. Fetch geometry from FeatureServer so Haversine matching works.

**Tech Stack:** Python, SQLite, Pandas, requests (ArcGIS FeatureServer)

---

### Task 1: Write failing test for `resolve_unknown_sites`

**Files:**
- Modify: `tests/data_processing/test_arcgis_sync.py`

**Step 1: Write the failing test**

Add a new test class at the bottom of the file. This tests the core behavior: unknown sites get resolved via normalization, alias, haversine, and auto-insert — no rows dropped.

```python
class TestResolveUnknownSites(unittest.TestCase):
    """Tests for resolve_unknown_sites (replaces filter_known_sites)."""

    def _setup_db(self):
        """Create an in-memory DB with sites table and test sites."""
        import sqlite3
        conn = sqlite3.connect(':memory:')
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('''
            CREATE TABLE sites (
                site_id INTEGER PRIMARY KEY AUTOINCREMENT,
                site_name TEXT NOT NULL UNIQUE,
                latitude REAL,
                longitude REAL,
                active INTEGER DEFAULT 1,
                source_file TEXT
            )
        ''')
        # Insert known sites
        conn.execute(
            "INSERT INTO sites (site_name, latitude, longitude) VALUES (?, ?, ?)",
            ('Coffee Creek: N. Sooner Rd', 35.5, -97.5),
        )
        conn.execute(
            "INSERT INTO sites (site_name, latitude, longitude) VALUES (?, ?, ?)",
            ('Boomer Creek: 3rd Ave', 36.1, -97.1),
        )
        conn.commit()
        return conn

    def test_exact_match_keeps_row(self):
        """Sites that exactly match the DB are kept as-is."""
        conn = self._setup_db()
        df = pd.DataFrame({'Site_Name': ['Coffee Creek: N. Sooner Rd'], 'value': [1]})
        result_df, stats = resolve_unknown_sites(df, conn)
        self.assertEqual(len(result_df), 1)
        self.assertEqual(stats['already_known'], 1)
        conn.close()

    def test_normalized_match_resolves(self):
        """Site names that differ only by whitespace/punctuation resolve via normalization."""
        conn = self._setup_db()
        # Extra period and double space — should normalize to match
        df = pd.DataFrame({'Site_Name': ['Coffee Creek:  N. Sooner Rd.'], 'value': [1]})
        result_df, stats = resolve_unknown_sites(df, conn)
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df['Site_Name'].iloc[0], 'Coffee Creek: N. Sooner Rd')
        self.assertEqual(stats['normalized_match'], 1)
        conn.close()

    def test_haversine_match_resolves(self):
        """Sites within 50m of an existing site resolve via coordinate matching."""
        conn = self._setup_db()
        # ~10m away from 'Coffee Creek: N. Sooner Rd' at (35.5, -97.5)
        df = pd.DataFrame({
            'Site_Name': ['Coffee Creek: New Name'],
            'latitude': [35.50009],
            'longitude': [-97.50009],
            'value': [1],
        })
        result_df, stats = resolve_unknown_sites(df, conn)
        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df['Site_Name'].iloc[0], 'Coffee Creek: N. Sooner Rd')
        self.assertEqual(stats['coordinate_match'], 1)
        conn.close()

    def test_auto_insert_creates_new_site(self):
        """Genuinely new sites are auto-inserted into the sites table."""
        conn = self._setup_db()
        df = pd.DataFrame({
            'Site_Name': ['Brand New Creek: Totally New'],
            'latitude': [34.0],
            'longitude': [-96.0],
            'value': [1],
        })
        result_df, stats = resolve_unknown_sites(df, conn)
        self.assertEqual(len(result_df), 1)
        self.assertEqual(stats['auto_inserted'], 1)
        # Verify site was actually inserted
        row = conn.execute(
            "SELECT site_name FROM sites WHERE site_name = ?",
            ('Brand New Creek: Totally New',),
        ).fetchone()
        self.assertIsNotNone(row)
        conn.close()

    def test_no_rows_dropped(self):
        """All rows are preserved — mix of known, normalized, and new sites."""
        conn = self._setup_db()
        df = pd.DataFrame({
            'Site_Name': [
                'Coffee Creek: N. Sooner Rd',       # exact match
                'Coffee Creek:  N. Sooner Rd.',      # normalized match
                'Brand New Creek: Somewhere',         # auto-insert
            ],
            'latitude': [None, None, 34.0],
            'longitude': [None, None, -96.0],
            'value': [1, 2, 3],
        })
        result_df, stats = resolve_unknown_sites(df, conn)
        self.assertEqual(len(result_df), 3, "No rows should be dropped")
        conn.close()
```

**Step 2: Add the import**

At the top of the test file, add `resolve_unknown_sites` to the imports:

```python
from data_processing.arcgis_sync import (
    fetch_site_data,
    prepare_dataframe,
    parse_epoch_dates,
    process_fetched_data,
    sync_all_chemical_data,
    _fetch_features_paginated,
    resolve_unknown_sites,  # <-- add this
)
```

**Step 3: Run the test to verify it fails**

Run: `pytest tests/data_processing/test_arcgis_sync.py::TestResolveUnknownSites -v`
Expected: `ImportError` — `resolve_unknown_sites` doesn't exist yet.

---

### Task 2: Implement `resolve_unknown_sites` in `arcgis_sync.py`

**Files:**
- Modify: `data_processing/arcgis_sync.py`

**Step 1: Add imports**

At the top of `arcgis_sync.py`, add to the existing `chemical_utils` import block (line 30-36):

```python
from data_processing.chemical_utils import (
    SITE_ALIASES,          # <-- add
    apply_bdl_conversions,
    calculate_soluble_nitrogen,
    insert_chemical_data,
    normalize_site_name,   # already imported on line 21, but consolidate here
    remove_empty_chemical_rows,
    validate_chemical_data,
)
from data_processing.merge_sites import haversine_m
```

Remove the standalone `from data_processing.chemical_utils import normalize_site_name` on line 21 since it's now in the consolidated import.

**Step 2: Add `resolve_unknown_sites` function**

Place this after `filter_known_sites()` (after line 533). This is the DataFrame-level wrapper that uses `resolve_unknown_site` logic inline.

```python
DISTANCE_THRESHOLD_M = 50.0


def resolve_unknown_sites(df, conn=None):
    """Resolve all site names in DataFrame, auto-inserting unknowns.

    Uses the same resolution chain as the Cloud Function's site_manager:
    1. Exact name match against DB
    2. Normalized name match (casefold + whitespace/punctuation)
    3. Alias lookup via SITE_ALIASES
    4. Haversine coordinate match (within 50m)
    5. Auto-insert into sites table

    Args:
        df: Processed DataFrame with 'Site_Name' column (and optional
            'latitude'/'longitude' columns).
        conn: Optional SQLite connection. If None, gets its own.

    Returns:
        Tuple of (resolved_df, stats_dict). All rows are preserved —
        unknown Site_Names are remapped to their resolved name.
    """
    if df.empty or 'Site_Name' not in df.columns:
        return df, {'already_known': 0, 'normalized_match': 0,
                     'alias_match': 0, 'coordinate_match': 0, 'auto_inserted': 0}

    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        # Load site lookup: {name: site_id} and coordinate data
        cursor = conn.cursor()
        cursor.execute("SELECT site_id, site_name, latitude, longitude FROM sites")
        all_sites = cursor.fetchall()
        site_lookup = {row[1]: row[0] for row in all_sites}

        unique_names = df['Site_Name'].unique()
        name_map = {}  # original_name -> resolved_db_name
        stats = {'already_known': 0, 'normalized_match': 0,
                 'alias_match': 0, 'coordinate_match': 0, 'auto_inserted': 0}

        has_coords = 'latitude' in df.columns and 'longitude' in df.columns

        for name in unique_names:
            # Step 1: Exact match
            if name in site_lookup:
                name_map[name] = name
                stats['already_known'] += 1
                continue

            # Step 2: Normalized match
            normalized = normalize_site_name(name).casefold()
            matched = False
            for db_name in site_lookup:
                if normalize_site_name(db_name).casefold() == normalized:
                    name_map[name] = db_name
                    stats['normalized_match'] += 1
                    matched = True
                    logger.info(f"Normalized match: '{name}' -> '{db_name}'")
                    break
            if matched:
                continue

            # Step 3: Alias lookup
            canonical = SITE_ALIASES.get(normalized)
            if canonical:
                # Try exact then normalized lookup of canonical name
                if canonical in site_lookup:
                    name_map[name] = canonical
                    stats['alias_match'] += 1
                    logger.info(f"Alias match: '{name}' -> '{canonical}'")
                    continue
                canonical_norm = normalize_site_name(canonical).casefold()
                alias_matched = False
                for db_name in site_lookup:
                    if normalize_site_name(db_name).casefold() == canonical_norm:
                        name_map[name] = db_name
                        stats['alias_match'] += 1
                        logger.info(f"Alias match: '{name}' -> '{db_name}'")
                        alias_matched = True
                        break
                if alias_matched:
                    continue

            # Step 4: Haversine coordinate match
            if has_coords:
                # Get first non-null lat/lon for this site name
                site_rows = df[df['Site_Name'] == name]
                lat = site_rows['latitude'].dropna().iloc[0] if not site_rows['latitude'].dropna().empty else None
                lon = site_rows['longitude'].dropna().iloc[0] if not site_rows['longitude'].dropna().empty else None

                if lat is not None and lon is not None:
                    coord_matched = False
                    for site_id, db_name, ex_lat, ex_lon in all_sites:
                        if ex_lat is None or ex_lon is None:
                            continue
                        dist = haversine_m(lat, lon, ex_lat, ex_lon)
                        if dist <= DISTANCE_THRESHOLD_M:
                            name_map[name] = db_name
                            stats['coordinate_match'] += 1
                            logger.info(
                                f"Coordinate match: '{name}' -> '{db_name}' ({dist:.1f}m)"
                            )
                            coord_matched = True
                            break
                    if coord_matched:
                        continue

            # Step 5: Auto-insert
            lat_val = None
            lon_val = None
            if has_coords:
                site_rows = df[df['Site_Name'] == name]
                lat_val = site_rows['latitude'].dropna().iloc[0] if not site_rows['latitude'].dropna().empty else None
                lon_val = site_rows['longitude'].dropna().iloc[0] if not site_rows['longitude'].dropna().empty else None

            cursor.execute(
                "INSERT OR IGNORE INTO sites (site_name, latitude, longitude, active) VALUES (?, ?, ?, 1)",
                (name, lat_val, lon_val),
            )
            conn.commit()
            # Refresh lookup with new site
            cursor.execute("SELECT site_id FROM sites WHERE site_name = ?", (name,))
            new_id = cursor.fetchone()[0]
            site_lookup[name] = new_id
            name_map[name] = name
            stats['auto_inserted'] += 1
            logger.info(f"Auto-inserted new site: '{name}' (site_id={new_id})")

        # Apply name remapping to DataFrame
        resolved_df = df.copy()
        resolved_df['Site_Name'] = resolved_df['Site_Name'].map(name_map)

        total_resolved = stats['normalized_match'] + stats['alias_match'] + stats['coordinate_match']
        logger.info(
            f"Site resolution complete: {stats['already_known']} known, "
            f"{total_resolved} resolved, {stats['auto_inserted']} auto-inserted"
        )

        return resolved_df, stats

    finally:
        if own_conn:
            close_connection(conn)
```

**Step 3: Run the tests**

Run: `pytest tests/data_processing/test_arcgis_sync.py::TestResolveUnknownSites -v`
Expected: All 5 tests PASS.

**Step 4: Commit**

```bash
git add data_processing/arcgis_sync.py tests/data_processing/test_arcgis_sync.py
git commit -m "feat: add resolve_unknown_sites to arcgis_sync"
```

---

### Task 3: Wire into `sync_all_chemical_data` and enable geometry fetch

**Files:**
- Modify: `data_processing/arcgis_sync.py` (lines 732, 767-787)

**Step 1: Write the failing integration test**

Add to `tests/data_processing/test_arcgis_sync.py`:

```python
class TestSyncAllResolvesUnknownSites(unittest.TestCase):
    """Verify sync_all_chemical_data uses resolve_unknown_sites, not filter_known_sites."""

    @patch('data_processing.arcgis_sync._fetch_features_paginated')
    @patch('data_processing.arcgis_sync.insert_chemical_data')
    @patch('data_processing.arcgis_sync.get_connection')
    @patch('data_processing.arcgis_sync.close_connection')
    def test_unknown_sites_not_dropped(self, mock_close, mock_conn, mock_insert, mock_fetch):
        """sync_all_chemical_data should resolve unknown sites, not skip them."""
        import sqlite3

        # Set up in-memory DB with one known site
        conn = sqlite3.connect(':memory:')
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('''
            CREATE TABLE sites (
                site_id INTEGER PRIMARY KEY AUTOINCREMENT,
                site_name TEXT NOT NULL UNIQUE,
                latitude REAL, longitude REAL,
                active INTEGER DEFAULT 1, source_file TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE chemical_collection_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                site_id INTEGER REFERENCES sites(site_id),
                collection_date TEXT, year INTEGER, month INTEGER,
                sample_id INTEGER, data_source TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE chemical_measurements (
                measurement_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER REFERENCES chemical_collection_events(event_id),
                parameter_id INTEGER, value REAL, status TEXT
            )
        ''')
        conn.execute(
            "INSERT INTO sites (site_name, latitude, longitude) VALUES (?, ?, ?)",
            ('Known Creek: Main St', 35.5, -97.5),
        )
        conn.commit()
        mock_conn.return_value = conn

        # Fake Feature Server response — one known site, one unknown
        mock_fetch.return_value = [
            {
                'attributes': {
                    'objectid': 1, 'SiteName': 'Known Creek: Main St',
                    'day': 1742025600000, 'oxygen_sat': 95,
                    'pH1': 7.0, 'pH2': 7.1,
                    'nitratetest1': 0.5, 'nitratetest2': 0.6,
                    'nitritetest1': 0.01, 'nitritetest2': 0.02,
                    'Ammonia_Range': 'Low', 'ammonia_Nitrogen2': 0.1, 'ammonia_Nitrogen3': 0.1,
                    'Ammonia_nitrogen_midrange1_Final': None, 'Ammonia_nitrogen_midrange2_Final': None,
                    'Ortho_Range': 'Low',
                    'Orthophosphate_Low1_Final': 0.02, 'Orthophosphate_Low2_Final': 0.02,
                    'Orthophosphate_Mid1_Final': None, 'Orthophosphate_Mid2_Final': None,
                    'Orthophosphate_High1_Final': None, 'Orthophosphate_High2_Final': None,
                    'Chloride_Range': 'Low',
                    'Chloride_Low1_Final': 10, 'Chloride_Low2_Final': 10,
                    'Chloride_High1_Final': None, 'Chloride_High2_Final': None,
                    'QAQC_Complete': 'X',
                },
                'geometry': {'x': -97.5, 'y': 35.5},
            },
            {
                'attributes': {
                    'objectid': 2, 'SiteName': 'Unknown Creek: New Site',
                    'day': 1742025600000, 'oxygen_sat': 88,
                    'pH1': 7.5, 'pH2': 7.4,
                    'nitratetest1': 0.3, 'nitratetest2': 0.3,
                    'nitritetest1': 0.01, 'nitritetest2': 0.01,
                    'Ammonia_Range': 'Low', 'ammonia_Nitrogen2': 0.05, 'ammonia_Nitrogen3': 0.05,
                    'Ammonia_nitrogen_midrange1_Final': None, 'Ammonia_nitrogen_midrange2_Final': None,
                    'Ortho_Range': 'Low',
                    'Orthophosphate_Low1_Final': 0.01, 'Orthophosphate_Low2_Final': 0.01,
                    'Orthophosphate_Mid1_Final': None, 'Orthophosphate_Mid2_Final': None,
                    'Orthophosphate_High1_Final': None, 'Orthophosphate_High2_Final': None,
                    'Chloride_Range': 'Low',
                    'Chloride_Low1_Final': 5, 'Chloride_Low2_Final': 5,
                    'Chloride_High1_Final': None, 'Chloride_High2_Final': None,
                    'QAQC_Complete': 'X',
                },
                'geometry': {'x': -96.0, 'y': 34.0},
            },
        ]

        mock_insert.return_value = {'measurements_added': 2, 'events_added': 2, 'sites_processed': 2}

        result = sync_all_chemical_data()

        self.assertEqual(result['status'], 'success')
        # The unknown site should have been auto-inserted, not skipped
        self.assertEqual(result.get('sites_auto_inserted', 0), 1)
        # No skipped sites
        self.assertFalse(result.get('skipped_sites'))

        # Verify insert_chemical_data was called with BOTH rows (not just the known one)
        call_args = mock_insert.call_args
        inserted_df = call_args[0][0]
        self.assertEqual(len(inserted_df), 2)

        conn.close()
```

**Step 2: Run to verify it fails**

Run: `pytest tests/data_processing/test_arcgis_sync.py::TestSyncAllResolvesUnknownSites -v`
Expected: FAIL — `sync_all_chemical_data` still uses `filter_known_sites`.

**Step 3: Enable geometry in `sync_all_chemical_data`**

In `sync_all_chemical_data()`, change the `_fetch_features_paginated` call (line 732-736) to request geometry:

```python
    records = _fetch_features_paginated(
        where="QAQC_Complete IS NOT NULL",
        out_fields=CHEMICAL_FIELDS,
        order_by_fields='day ASC',
        return_geometry=True,
    )
```

**Step 4: Replace `filter_known_sites` with `resolve_unknown_sites`**

Replace lines 767-787 (the `filter_known_sites` call, dry_run block, and empty check) with:

```python
    conn = get_connection()
    try:
        resolved_df, site_stats = resolve_unknown_sites(processed_df, conn)
    finally:
        close_connection(conn)

    if dry_run:
        logger.info(f"DRY RUN: would insert {len(resolved_df)} records")
        return {
            'status': 'dry_run',
            'records_fetched': len(records),
            'records_after_processing': len(processed_df),
            'records_ready': len(resolved_df),
            'site_resolution': site_stats,
            'execution_time': str(datetime.now() - start_time),
        }

    if resolved_df.empty:
        return {
            'status': 'success',
            'records_fetched': len(records),
            'records_inserted': 0,
            'site_resolution': site_stats,
            'execution_time': str(datetime.now() - start_time),
        }

    stats = insert_chemical_data(resolved_df, data_source="arcgis_feature_server")

    result = {
        'status': 'success',
        'records_fetched': len(records),
        'records_after_processing': len(processed_df),
        'records_inserted': stats.get('measurements_added', 0),
        'events_added': stats.get('events_added', 0),
        'sites_processed': stats.get('sites_processed', 0),
        'sites_auto_inserted': site_stats.get('auto_inserted', 0),
        'site_resolution': site_stats,
        'execution_time': str(datetime.now() - start_time),
    }
```

**Step 5: Run all tests**

Run: `pytest tests/data_processing/test_arcgis_sync.py -v`
Expected: All tests PASS (including the new integration test).

**Step 6: Commit**

```bash
git add data_processing/arcgis_sync.py tests/data_processing/test_arcgis_sync.py
git commit -m "feat: wire resolve_unknown_sites into sync_all_chemical_data"
```

---

### Task 4: Run `reset_database` and verify results

**Files:**
- None (verification only)

**Step 1: Run reset_database**

Run: `python -m database.reset_database 2>&1 | tee /tmp/reset_output.log`

Watch the logs for:
- `Normalized match:` lines (name mismatches resolved)
- `Alias match:` lines
- `Coordinate match:` lines (haversine catches)
- `Auto-inserted new site:` lines (genuinely new)
- `Site resolution complete:` summary line

**Step 2: Verify no sites were dropped**

Run: `grep -c "Skipping.*unknown site" /tmp/reset_output.log`
Expected: 0 (the old `filter_known_sites` warning should no longer appear)

**Step 3: Check the 33 previously-dropped sites**

Run a quick query to verify some of the known dropped sites now exist:

```bash
python -c "
from database.database import get_connection, close_connection
conn = get_connection()
for name in ['Coffee Creek: N. Sooner Rd', 'Boomer Creek: 3rd Ave', 'Dog Creek: Blue Starr Dr']:
    row = conn.execute('SELECT site_id, site_name FROM sites WHERE site_name = ?', (name,)).fetchone()
    print(f'{name}: {row}')
close_connection(conn)
"
```

**Step 4: Compare total chemical record counts**

Check that the total measurement count increased (should have ~33 sites' worth of additional data):

```bash
python -c "
from database.database import get_connection, close_connection
conn = get_connection()
events = conn.execute('SELECT COUNT(*) FROM chemical_collection_events').fetchone()[0]
measurements = conn.execute('SELECT COUNT(*) FROM chemical_measurements').fetchone()[0]
sites = conn.execute('SELECT COUNT(*) FROM sites').fetchone()[0]
print(f'Sites: {sites}, Events: {events}, Measurements: {measurements}')
close_connection(conn)
"
```

**Step 5: Commit if satisfied**

```bash
git add -A
git commit -m "fix: resolve unknown sites in local reset (33 sites no longer dropped)"
```

---

### Notes

- **`filter_known_sites()` can be left in place** as dead code for now. It's not called after the change. Clean it up later if desired.
- **Geometry fetch adds latency** to `sync_all_chemical_data` (more data per request). This is acceptable for a one-time reset. If you ever re-run it, the extra data is small.
- **The Cloud Function's `site_manager.py` is unchanged.** It continues working independently for daily syncs.
- **After uploading the DB to GCS**, the Cloud Function picks up from the latest state. The auto-inserted sites will already exist, so the cloud resolution chain will hit exact-match for them going forward.
