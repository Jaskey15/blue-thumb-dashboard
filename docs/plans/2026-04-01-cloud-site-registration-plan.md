# Cloud Site Registration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Automatically detect new monitoring sites during cloud sync, stage them for human review in a `pending_sites` table, and handle coordinate-based duplicate detection so data isn't silently dropped.

**Architecture:** New `site_manager.py` in the cloud function handles site resolution (Haversine dedup + pending staging) and promotion of approved sites. `chemical_processor.py` calls into it when encountering unknown sites. `main.py` orchestrates promotion before fetch so approved sites get data in the same sync cycle.

**Tech Stack:** Python 3.12 | SQLite | Pandas | Haversine distance (from `merge_sites.py`)

---

### Task 1: Extract `haversine_m` to module level in `merge_sites.py`

Currently a nested function inside `find_duplicate_coordinate_groups` (line 105). Must be module-level to be importable by `site_manager.py`.

**Files:**
- Modify: `data_processing/merge_sites.py:105-111` (move function up)
- Test: `tests/data_processing/test_merge_sites.py` (if exists, verify no regression)

**Step 1: Move `haversine_m` from nested to module-level**

Cut the function from inside `find_duplicate_coordinate_groups` and place it above the function as a module-level function:

```python
def haversine_m(lat1, lon1, lat2, lon2):
    """Great-circle distance between two lat/lon points in meters."""
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))
```

Inside `find_duplicate_coordinate_groups`, remove the nested definition — the existing calls to `haversine_m` will resolve to the module-level function.

**Step 2: Run existing tests**

Run: `pytest tests/data_processing/ -v -k merge`
Expected: All existing merge_sites tests pass (behavior unchanged).

**Step 3: Commit**

```bash
git add data_processing/merge_sites.py
git commit -m "refactor: extract haversine_m to module level for reuse"
```

---

### Task 2: Add `pending_sites` table to `db_schema.py`

**Files:**
- Modify: `database/db_schema.py` (add table creation after habitat tables, before indexes)
- Test: `tests/database/test_db_schema.py` (if exists)

**Step 1: Write the failing test**

Create `tests/database/test_pending_sites_schema.py`:

```python
"""Tests for pending_sites table schema."""
import sqlite3
import tempfile
import unittest
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)


class TestPendingSitesSchema(unittest.TestCase):
    """Verify pending_sites table is created with correct schema."""

    def test_pending_sites_table_exists(self):
        """pending_sites table should be created by create_tables()."""
        from database.db_schema import create_tables
        from database.database import get_connection, close_connection

        create_tables()
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='pending_sites'"
            )
            result = cursor.fetchone()
            self.assertIsNotNone(result, "pending_sites table should exist")
        finally:
            close_connection(conn)

    def test_pending_sites_unique_site_name(self):
        """Inserting duplicate site_name should fail."""
        from database.db_schema import create_tables
        from database.database import get_connection, close_connection

        create_tables()
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO pending_sites (site_name, first_seen_date) VALUES (?, ?)",
                ("Test Creek", "2026-04-01"),
            )
            with self.assertRaises(sqlite3.IntegrityError):
                cursor.execute(
                    "INSERT INTO pending_sites (site_name, first_seen_date) VALUES (?, ?)",
                    ("Test Creek", "2026-04-02"),
                )
        finally:
            close_connection(conn)

    def test_pending_sites_columns(self):
        """Verify all expected columns exist with correct types."""
        from database.db_schema import create_tables
        from database.database import get_connection, close_connection

        create_tables()
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(pending_sites)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}
            expected = {
                'pending_site_id': 'INTEGER',
                'site_name': 'TEXT',
                'latitude': 'REAL',
                'longitude': 'REAL',
                'first_seen_date': 'TEXT',
                'source': 'TEXT',
                'status': 'TEXT',
                'reviewed_date': 'TEXT',
                'notes': 'TEXT',
                'nearest_site_name': 'TEXT',
                'nearest_site_distance_m': 'REAL',
            }
            for col, col_type in expected.items():
                self.assertIn(col, columns, f"Column {col} should exist")
                self.assertEqual(columns[col], col_type, f"Column {col} should be {col_type}")
        finally:
            close_connection(conn)


if __name__ == '__main__':
    unittest.main()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/database/test_pending_sites_schema.py -v`
Expected: FAIL — `pending_sites` table does not exist.

**Step 3: Add the table to `db_schema.py`**

In `database/db_schema.py`, after the habitat_summary_scores table (after line 281) and before the index creation block (line 283), add:

```python
    # ---------- PENDING SITES TABLE ----------
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pending_sites (
        pending_site_id INTEGER PRIMARY KEY,
        site_name TEXT NOT NULL,
        latitude REAL,
        longitude REAL,
        first_seen_date TEXT NOT NULL,
        source TEXT DEFAULT 'feature_server',
        status TEXT DEFAULT 'pending',
        reviewed_date TEXT,
        notes TEXT,
        nearest_site_name TEXT,
        nearest_site_distance_m REAL,
        UNIQUE(site_name)
    )
    ''')
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/database/test_pending_sites_schema.py -v`
Expected: PASS

**Step 5: Run full database test suite**

Run: `pytest tests/database/ -v`
Expected: All pass (no regressions).

**Step 6: Commit**

```bash
git add database/db_schema.py tests/database/test_pending_sites_schema.py
git commit -m "feat: add pending_sites table for cloud site registration"
```

---

### Task 3: Add geometry extraction to `arcgis_sync.py`

**Files:**
- Modify: `data_processing/arcgis_sync.py:183-215` (`_fetch_features_paginated`)
- Test: `tests/data_processing/test_arcgis_sync.py` (if exists, otherwise new)

**Step 1: Write the failing test**

Create or add to `tests/data_processing/test_arcgis_geometry.py`:

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
    """Verify lat/lon are extracted from FeatureServer geometry."""

    @patch('data_processing.arcgis_sync.requests.get')
    def test_geometry_extracted_into_attributes(self, mock_get):
        """Geometry x/y should be injected as longitude/latitude in records."""
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
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['longitude'], -97.5)
        self.assertEqual(records[0]['latitude'], 35.4)

    @patch('data_processing.arcgis_sync.requests.get')
    def test_missing_geometry_gives_none(self, mock_get):
        """Records without geometry should have latitude/longitude as None."""
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
        self.assertIsNone(records[0]['latitude'])
        self.assertIsNone(records[0]['longitude'])

    @patch('data_processing.arcgis_sync.requests.get')
    def test_return_geometry_param_sent(self, mock_get):
        """The returnGeometry param should be included in the request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'features': []}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        _fetch_features_paginated(
            where="1=1",
            out_fields=['objectid'],
            order_by_fields='objectid',
        )

        call_args = mock_get.call_args
        params = call_args.kwargs.get('params') or call_args[1].get('params')
        self.assertEqual(params.get('returnGeometry'), 'true')


if __name__ == '__main__':
    unittest.main()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/data_processing/test_arcgis_geometry.py -v`
Expected: FAIL — `returnGeometry` not in params, latitude/longitude not in records.

**Step 3: Modify `_fetch_features_paginated` in `arcgis_sync.py`**

In `data_processing/arcgis_sync.py`, two changes:

1. Add `returnGeometry` to params dict (after line 190):

```python
params = {
    'where': where,
    'outFields': ','.join(out_fields),
    'f': 'json',
    'orderByFields': order_by_fields,
    'resultRecordCount': page_size,
    'resultOffset': result_offset,
    'returnGeometry': 'true',
}
```

2. Replace the feature extraction loop (lines 212-215) to also extract geometry:

```python
for f in features:
    attrs = f.get('attributes') if isinstance(f, dict) else None
    if isinstance(attrs, dict):
        geom = f.get('geometry') or {}
        attrs['latitude'] = geom.get('y')
        attrs['longitude'] = geom.get('x')
        records.append(attrs)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/data_processing/test_arcgis_geometry.py -v`
Expected: PASS

**Step 5: Run full arcgis_sync tests**

Run: `pytest tests/data_processing/ -v -k arcgis`
Expected: All pass.

**Step 6: Commit**

```bash
git add data_processing/arcgis_sync.py tests/data_processing/test_arcgis_geometry.py
git commit -m "feat: extract geometry from FeatureServer responses"
```

---

### Task 4: Preserve lat/lon through chemical processing pipeline

**Critical finding:** `format_to_database_schema()` in `updated_chemical_processing.py:312-318` explicitly selects only required columns, dropping any extras including lat/lon. We need to conditionally preserve them.

**Files:**
- Modify: `data_processing/updated_chemical_processing.py:312-318`
- Test: existing tests for `format_to_database_schema`

**Step 1: Write the failing test**

Add to an appropriate test file (or create `tests/data_processing/test_latlon_passthrough.py`):

```python
"""Tests that lat/lon columns survive the chemical processing pipeline."""
import unittest
import os
import sys

import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from data_processing.updated_chemical_processing import format_to_database_schema


class TestLatLonPassthrough(unittest.TestCase):
    """Verify latitude/longitude columns are preserved through formatting."""

    def _make_sample_df(self, include_coords=True):
        """Create a minimal valid DataFrame for format_to_database_schema."""
        data = {
            'Site Name': ['Test Creek'],
            'Date': [pd.Timestamp('2026-01-15')],
            'Year': [2026],
            'Month': [1],
            '% Oxygen Saturation': [95.0],
            'pH #1': [7.2],
            'pH #2': [7.3],
            'Nitrate': [1.0],
            'Nitrite': [0.1],
            'Ammonia': [0.5],
            'Orthophosphate': [0.05],
            'Chloride': [25.0],
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

**Step 2: Run test to verify it fails**

Run: `pytest tests/data_processing/test_latlon_passthrough.py -v`
Expected: FAIL — `latitude` not in result columns.

**Step 3: Modify `format_to_database_schema` in `updated_chemical_processing.py`**

At line ~315, after the `sample_id` check, add:

```python
if has_sample_id:
    required_columns.append('sample_id')

# Preserve geometry columns for cloud sync site resolution
for geo_col in ('latitude', 'longitude'):
    if geo_col in formatted_df.columns:
        required_columns.append(geo_col)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/data_processing/test_latlon_passthrough.py -v`
Expected: PASS

**Step 5: Run full processing tests**

Run: `pytest tests/data_processing/ -v`
Expected: All pass (no regressions).

**Step 6: Commit**

```bash
git add data_processing/updated_chemical_processing.py tests/data_processing/test_latlon_passthrough.py
git commit -m "feat: preserve lat/lon columns through chemical processing pipeline"
```

---

### Task 5: Create `site_manager.py`

**Files:**
- Create: `cloud_functions/survey123_sync/site_manager.py`
- Test: `tests/survey123_sync/test_site_manager.py`

**Step 1: Write the failing tests**

Create `tests/survey123_sync/test_site_manager.py`:

```python
"""Tests for site_manager: resolve unknown sites and promote approved pending sites."""
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import MagicMock

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'cloud_functions', 'survey123_sync'))

# Mock Cloud Function specific modules
sys.modules['functions_framework'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()


def _create_test_db(path):
    """Create a minimal test database with sites and pending_sites tables."""
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE sites (
            site_id INTEGER PRIMARY KEY,
            site_name TEXT NOT NULL UNIQUE,
            latitude REAL,
            longitude REAL,
            county TEXT,
            river_basin TEXT,
            ecoregion TEXT,
            active BOOLEAN DEFAULT 1,
            last_chemical_reading_date TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE pending_sites (
            pending_site_id INTEGER PRIMARY KEY,
            site_name TEXT NOT NULL,
            latitude REAL,
            longitude REAL,
            first_seen_date TEXT NOT NULL,
            source TEXT DEFAULT 'feature_server',
            status TEXT DEFAULT 'pending',
            reviewed_date TEXT,
            notes TEXT,
            nearest_site_name TEXT,
            nearest_site_distance_m REAL,
            UNIQUE(site_name)
        )
    ''')
    # Insert some existing sites
    cursor.execute(
        "INSERT INTO sites (site_id, site_name, latitude, longitude) VALUES (1, 'Bull Creek: Main', 35.4, -97.5)"
    )
    cursor.execute(
        "INSERT INTO sites (site_id, site_name, latitude, longitude) VALUES (2, 'Clear Creek: Bridge', 35.8, -97.1)"
    )
    conn.commit()
    return conn


class TestResolveUnknownSite(unittest.TestCase):
    """Test resolve_unknown_site function."""

    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.conn = _create_test_db(self.temp_db.name)

    def tearDown(self):
        self.conn.close()
        os.unlink(self.temp_db.name)

    def _get_existing_sites(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT site_id, site_name, latitude, longitude FROM sites WHERE latitude IS NOT NULL")
        return cursor.fetchall()

    def test_coordinate_match_returns_site_id(self):
        """Site within 50m of existing should return the existing site_id."""
        from site_manager import resolve_unknown_site

        existing = self._get_existing_sites()
        # 0.0001 degrees ~ 11m at this latitude
        result = resolve_unknown_site(
            'Bull Creek: Alternate', 35.4001, -97.5001, existing, self.conn
        )
        self.assertEqual(result, 1)  # Matched to Bull Creek: Main

    def test_no_match_inserts_pending(self):
        """Site far from all existing should return None and insert into pending_sites."""
        from site_manager import resolve_unknown_site

        existing = self._get_existing_sites()
        result = resolve_unknown_site(
            'New Creek: Remote', 36.0, -96.0, existing, self.conn
        )
        self.assertIsNone(result)

        cursor = self.conn.cursor()
        cursor.execute("SELECT site_name, status FROM pending_sites WHERE site_name = 'New Creek: Remote'")
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[1], 'pending')

    def test_pending_records_nearest_site(self):
        """Pending site should record nearest existing site name and distance."""
        from site_manager import resolve_unknown_site

        existing = self._get_existing_sites()
        resolve_unknown_site('Far Creek', 36.0, -96.0, existing, self.conn)

        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT nearest_site_name, nearest_site_distance_m FROM pending_sites WHERE site_name = 'Far Creek'"
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row[0])  # nearest_site_name should be set
        self.assertIsNotNone(row[1])  # distance should be set
        self.assertGreater(row[1], 50)  # should be > 50m (no match)

    def test_duplicate_pending_ignored(self):
        """Second call for same site_name should not raise (INSERT OR IGNORE)."""
        from site_manager import resolve_unknown_site

        existing = self._get_existing_sites()
        resolve_unknown_site('New Creek', 36.0, -96.0, existing, self.conn)
        resolve_unknown_site('New Creek', 36.0, -96.0, existing, self.conn)

        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM pending_sites WHERE site_name = 'New Creek'")
        self.assertEqual(cursor.fetchone()[0], 1)

    def test_no_coordinates_inserts_pending(self):
        """Site with None coordinates should go to pending without Haversine check."""
        from site_manager import resolve_unknown_site

        existing = self._get_existing_sites()
        result = resolve_unknown_site('No Coords Creek', None, None, existing, self.conn)
        self.assertIsNone(result)

        cursor = self.conn.cursor()
        cursor.execute("SELECT site_name FROM pending_sites WHERE site_name = 'No Coords Creek'")
        self.assertIsNotNone(cursor.fetchone())


class TestPromoteApprovedSites(unittest.TestCase):
    """Test promote_approved_sites function."""

    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.conn = _create_test_db(self.temp_db.name)

    def tearDown(self):
        self.conn.close()
        os.unlink(self.temp_db.name)

    def test_approved_site_promoted(self):
        """Approved pending site should be inserted into sites table."""
        from site_manager import promote_approved_sites

        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO pending_sites (site_name, latitude, longitude, first_seen_date, status) "
            "VALUES ('New Creek: Approved', 35.9, -97.0, '2026-04-01', 'approved')"
        )
        self.conn.commit()

        result = promote_approved_sites(self.conn)

        self.assertEqual(result['promoted'], 1)
        cursor.execute("SELECT site_name, latitude, longitude, active FROM sites WHERE site_name = 'New Creek: Approved'")
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertAlmostEqual(row[1], 35.9)
        self.assertEqual(row[3], 1)  # active

    def test_pending_site_not_promoted(self):
        """Sites still in pending status should not be promoted."""
        from site_manager import promote_approved_sites

        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO pending_sites (site_name, first_seen_date, status) "
            "VALUES ('Still Pending Creek', '2026-04-01', 'pending')"
        )
        self.conn.commit()

        result = promote_approved_sites(self.conn)
        self.assertEqual(result['promoted'], 0)

    def test_promoted_site_status_updated(self):
        """Promoted site's status in pending_sites should be updated."""
        from site_manager import promote_approved_sites

        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO pending_sites (site_name, latitude, longitude, first_seen_date, status) "
            "VALUES ('Promote Me Creek', 35.5, -97.2, '2026-04-01', 'approved')"
        )
        self.conn.commit()

        promote_approved_sites(self.conn)

        cursor.execute("SELECT status FROM pending_sites WHERE site_name = 'Promote Me Creek'")
        row = cursor.fetchone()
        self.assertEqual(row[0], 'promoted')

    def test_no_approved_sites(self):
        """When no approved sites exist, should return promoted=0."""
        from site_manager import promote_approved_sites

        result = promote_approved_sites(self.conn)
        self.assertEqual(result['promoted'], 0)
        self.assertEqual(result['names'], [])


if __name__ == '__main__':
    unittest.main()
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/survey123_sync/test_site_manager.py -v`
Expected: FAIL — `site_manager` module does not exist.

**Step 3: Create `site_manager.py`**

Create `cloud_functions/survey123_sync/site_manager.py`:

```python
"""
Site lifecycle management for cloud sync.

Handles two responsibilities:
1. Resolving unknown sites encountered during sync (Haversine dedup + pending staging)
2. Promoting approved pending sites to the active sites table
"""

import logging
import os
import sys
from datetime import datetime

_candidate_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)
)
if (
    os.path.isdir(os.path.join(_candidate_root, 'data_processing'))
    and _candidate_root not in sys.path
):
    sys.path.insert(0, _candidate_root)

from data_processing.merge_sites import haversine_m

logger = logging.getLogger(__name__)

DISTANCE_THRESHOLD_M = 50.0


def resolve_unknown_site(site_name, latitude, longitude, existing_sites, conn):
    """Resolve an unknown site name against existing sites.

    Checks if the unknown site is a coordinate duplicate of an existing site
    (within 50m). If so, returns the existing site_id. Otherwise, stages the
    site in pending_sites and returns None.

    Args:
        site_name: The unknown site name.
        latitude: Latitude from FeatureServer geometry (may be None).
        longitude: Longitude from FeatureServer geometry (may be None).
        existing_sites: List of (site_id, site_name, lat, lon) tuples from sites table.
        conn: SQLite connection (caller manages transaction).

    Returns:
        site_id if coordinate-matched to an existing site, None if staged as pending.
    """
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

    # No coordinate match — stage as pending
    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    cursor.execute(
        """
        INSERT OR IGNORE INTO pending_sites
            (site_name, latitude, longitude, first_seen_date, source, status,
             nearest_site_name, nearest_site_distance_m)
        VALUES (?, ?, ?, ?, 'feature_server', 'pending', ?, ?)
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


def promote_approved_sites(conn):
    """Move approved pending sites into the sites table.

    Args:
        conn: SQLite connection (caller manages transaction).

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
        conn.commit()

    return {'promoted': len(promoted_names), 'names': promoted_names}


def get_pending_site_summary(conn):
    """Get a summary of pending sites for the sync response.

    Args:
        conn: SQLite connection.

    Returns:
        Dict with total_pending count.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM pending_sites WHERE status = 'pending'")
    total = cursor.fetchone()[0]
    return {'total_pending': total}
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/survey123_sync/test_site_manager.py -v`
Expected: All PASS.

**Step 5: Commit**

```bash
git add cloud_functions/survey123_sync/site_manager.py tests/survey123_sync/test_site_manager.py
git commit -m "feat: add site_manager with resolve and promote logic"
```

---

### Task 6: Integrate site_manager into `chemical_processor.py`

**Files:**
- Modify: `cloud_functions/survey123_sync/chemical_processor.py:100-114`
- Test: `tests/survey123_sync/test_chemical_processor.py` (update existing tests + add new)

**Step 1: Write the failing test**

Add to `tests/survey123_sync/test_chemical_processor.py`:

```python
class TestUnknownSiteResolution(unittest.TestCase):
    """Test that unknown sites are resolved via site_manager."""

    def test_unknown_site_with_coord_match_gets_inserted(self):
        """Data for a coord-matched unknown site should be inserted under the matched site."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_db:
            try:
                conn = sqlite3.connect(temp_db.name)
                cursor = conn.cursor()

                # Create required tables
                cursor.execute('''CREATE TABLE sites (
                    site_id INTEGER PRIMARY KEY, site_name TEXT NOT NULL UNIQUE,
                    latitude REAL, longitude REAL, county TEXT, river_basin TEXT,
                    ecoregion TEXT, active BOOLEAN DEFAULT 1, last_chemical_reading_date TEXT)''')
                cursor.execute('''CREATE TABLE pending_sites (
                    pending_site_id INTEGER PRIMARY KEY, site_name TEXT NOT NULL,
                    latitude REAL, longitude REAL, first_seen_date TEXT NOT NULL,
                    source TEXT DEFAULT 'feature_server', status TEXT DEFAULT 'pending',
                    reviewed_date TEXT, notes TEXT, nearest_site_name TEXT,
                    nearest_site_distance_m REAL, UNIQUE(site_name))''')
                cursor.execute('''CREATE TABLE chemical_parameters (
                    parameter_id INTEGER PRIMARY KEY, parameter_name TEXT, parameter_code TEXT,
                    display_name TEXT, unit TEXT, UNIQUE(parameter_code))''')
                cursor.execute('''CREATE TABLE chemical_reference_values (
                    reference_id INTEGER PRIMARY KEY, parameter_id INTEGER, threshold_type TEXT, value REAL)''')
                cursor.execute('''CREATE TABLE chemical_collection_events (
                    event_id INTEGER PRIMARY KEY, site_id INTEGER, sample_id INTEGER,
                    collection_date TEXT, year INTEGER, month INTEGER)''')
                cursor.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_chemical_collection_events_sample_id
                    ON chemical_collection_events(sample_id) WHERE sample_id IS NOT NULL''')
                cursor.execute('''CREATE TABLE chemical_measurements (
                    event_id INTEGER, parameter_id INTEGER, value REAL, bdl_flag BOOLEAN DEFAULT 0,
                    status TEXT, PRIMARY KEY (event_id, parameter_id))''')

                # Insert site + reference data
                cursor.execute("INSERT INTO sites VALUES (1, 'Bull Creek: Main', 35.4, -97.5, NULL, NULL, NULL, 1, NULL)")
                cursor.executemany(
                    "INSERT INTO chemical_parameters VALUES (?, ?, ?, ?, ?)",
                    [(1,'DO','do_percent','DO','%'), (2,'pH','pH','pH','pH'), (3,'N','soluble_nitrogen','N','mg/L'),
                     (4,'P','Phosphorus','P','mg/L'), (5,'Cl','Chloride','Cl','mg/L')]
                )
                cursor.executemany(
                    "INSERT INTO chemical_reference_values VALUES (?, ?, ?, ?)",
                    [(1,1,'normal_min',80), (2,1,'normal_max',130), (3,2,'normal_min',6.5), (4,2,'normal_max',9.0),
                     (5,3,'normal',0.8), (6,3,'caution',1.5), (7,4,'normal',0.05), (8,4,'caution',0.1),
                     (9,5,'normal',200), (10,5,'caution',400)]
                )
                conn.commit()
                conn.close()

                # Data with unknown site name but coords matching Bull Creek: Main
                df = pd.DataFrame({
                    'Site_Name': ['Bull Creek: Alternate Name'],
                    'Date': [pd.Timestamp('2026-03-15')],
                    'Year': [2026], 'Month': [3],
                    'do_percent': [95.0], 'pH': [7.2],
                    'soluble_nitrogen': [0.5], 'Phosphorus': [0.04], 'Chloride': [20.0],
                    'sample_id': [999],
                    'latitude': [35.4001],   # ~11m from Bull Creek: Main
                    'longitude': [-97.5001],
                })

                result = insert_processed_data_to_db(df, temp_db.name)
                self.assertGreater(result['records_inserted'], 0)

                # Verify it was inserted under site_id 1
                conn = sqlite3.connect(temp_db.name)
                cursor = conn.cursor()
                cursor.execute("SELECT site_id FROM chemical_collection_events WHERE sample_id = 999")
                row = cursor.fetchone()
                self.assertEqual(row[0], 1)
                conn.close()
            finally:
                os.unlink(temp_db.name)

    def test_unknown_site_no_match_goes_to_pending(self):
        """Unknown site far from all existing should be staged in pending_sites."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_db:
            try:
                conn = sqlite3.connect(temp_db.name)
                cursor = conn.cursor()

                cursor.execute('''CREATE TABLE sites (
                    site_id INTEGER PRIMARY KEY, site_name TEXT NOT NULL UNIQUE,
                    latitude REAL, longitude REAL, county TEXT, river_basin TEXT,
                    ecoregion TEXT, active BOOLEAN DEFAULT 1, last_chemical_reading_date TEXT)''')
                cursor.execute('''CREATE TABLE pending_sites (
                    pending_site_id INTEGER PRIMARY KEY, site_name TEXT NOT NULL,
                    latitude REAL, longitude REAL, first_seen_date TEXT NOT NULL,
                    source TEXT DEFAULT 'feature_server', status TEXT DEFAULT 'pending',
                    reviewed_date TEXT, notes TEXT, nearest_site_name TEXT,
                    nearest_site_distance_m REAL, UNIQUE(site_name))''')
                cursor.execute('''CREATE TABLE chemical_parameters (
                    parameter_id INTEGER PRIMARY KEY, parameter_name TEXT, parameter_code TEXT,
                    display_name TEXT, unit TEXT, UNIQUE(parameter_code))''')
                cursor.execute('''CREATE TABLE chemical_reference_values (
                    reference_id INTEGER PRIMARY KEY, parameter_id INTEGER, threshold_type TEXT, value REAL)''')
                cursor.execute('''CREATE TABLE chemical_collection_events (
                    event_id INTEGER PRIMARY KEY, site_id INTEGER, sample_id INTEGER,
                    collection_date TEXT, year INTEGER, month INTEGER)''')
                cursor.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_chemical_collection_events_sample_id
                    ON chemical_collection_events(sample_id) WHERE sample_id IS NOT NULL''')
                cursor.execute('''CREATE TABLE chemical_measurements (
                    event_id INTEGER, parameter_id INTEGER, value REAL, bdl_flag BOOLEAN DEFAULT 0,
                    status TEXT, PRIMARY KEY (event_id, parameter_id))''')

                cursor.execute("INSERT INTO sites VALUES (1, 'Bull Creek: Main', 35.4, -97.5, NULL, NULL, NULL, 1, NULL)")
                cursor.executemany(
                    "INSERT INTO chemical_parameters VALUES (?, ?, ?, ?, ?)",
                    [(1,'DO','do_percent','DO','%'), (2,'pH','pH','pH','pH'), (3,'N','soluble_nitrogen','N','mg/L'),
                     (4,'P','Phosphorus','P','mg/L'), (5,'Cl','Chloride','Cl','mg/L')]
                )
                cursor.executemany(
                    "INSERT INTO chemical_reference_values VALUES (?, ?, ?, ?)",
                    [(1,1,'normal_min',80), (2,1,'normal_max',130), (3,2,'normal_min',6.5), (4,2,'normal_max',9.0),
                     (5,3,'normal',0.8), (6,3,'caution',1.5), (7,4,'normal',0.05), (8,4,'caution',0.1),
                     (9,5,'normal',200), (10,5,'caution',400)]
                )
                conn.commit()
                conn.close()

                df = pd.DataFrame({
                    'Site_Name': ['Far Away Creek'],
                    'Date': [pd.Timestamp('2026-03-15')],
                    'Year': [2026], 'Month': [3],
                    'do_percent': [90.0], 'pH': [7.0],
                    'soluble_nitrogen': [0.3], 'Phosphorus': [0.02], 'Chloride': [15.0],
                    'sample_id': [888],
                    'latitude': [36.0],    # Far from Bull Creek
                    'longitude': [-96.0],
                })

                result = insert_processed_data_to_db(df, temp_db.name)
                self.assertEqual(result['records_inserted'], 0)

                # Verify it ended up in pending_sites
                conn = sqlite3.connect(temp_db.name)
                cursor = conn.cursor()
                cursor.execute("SELECT site_name, status FROM pending_sites WHERE site_name = 'Far Away Creek'")
                row = cursor.fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row[1], 'pending')
                conn.close()
            finally:
                os.unlink(temp_db.name)
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/survey123_sync/test_chemical_processor.py::TestUnknownSiteResolution -v`
Expected: FAIL — current code just skips unknown sites.

**Step 3: Modify `insert_processed_data_to_db` in `chemical_processor.py`**

Replace the unknown-site handling block (lines ~101-114). The full modified function section:

```python
from site_manager import resolve_unknown_site

# ... inside insert_processed_data_to_db, after site_lookup is built ...

        # Load existing site coordinates for Haversine checks
        coord_query = "SELECT site_id, site_name, latitude, longitude FROM sites WHERE latitude IS NOT NULL"
        coord_rows = cursor.execute(coord_query).fetchall()

        records_inserted = 0
        has_sample_id = 'sample_id' in df.columns
        has_coords = 'latitude' in df.columns and 'longitude' in df.columns
        resolved_cache = {}  # site_name -> site_id or None (avoid re-resolving)
        new_pending_names = []
        coordinate_matched = 0

        for _, row in df.iterrows():
            site_name = row['Site_Name']

            if site_name not in site_lookup:
                # Check cache first
                if site_name in resolved_cache:
                    resolved_id = resolved_cache[site_name]
                else:
                    lat = row.get('latitude') if has_coords else None
                    lon = row.get('longitude') if has_coords else None
                    resolved_id = resolve_unknown_site(
                        site_name, lat, lon, coord_rows, conn
                    )
                    resolved_cache[site_name] = resolved_id
                    if resolved_id is not None:
                        coordinate_matched += 1
                        site_lookup[site_name] = resolved_id
                    else:
                        new_pending_names.append(site_name)

                if resolved_id is None:
                    continue

            site_id = site_lookup[site_name]
            # ... rest of insertion logic unchanged ...
```

Also update the return dict to include pending info:

```python
        result = {'records_inserted': records_inserted}
        if new_pending_names:
            result['new_pending'] = list(set(new_pending_names))
        if coordinate_matched:
            result['coordinate_matched'] = coordinate_matched
        return result
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/survey123_sync/test_chemical_processor.py -v`
Expected: All PASS (existing + new tests).

**Step 5: Commit**

```bash
git add cloud_functions/survey123_sync/chemical_processor.py tests/survey123_sync/test_chemical_processor.py
git commit -m "feat: integrate site_manager into chemical processor for unknown site resolution"
```

---

### Task 7: Add orchestration to `main.py`

**Files:**
- Modify: `cloud_functions/survey123_sync/main.py:146-290`

**Step 1: Write the failing test**

Add to `tests/survey123_sync/test_main_pending_sites.py`:

```python
"""Tests for pending site orchestration in main.py."""
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'cloud_functions', 'survey123_sync'))

sys.modules['functions_framework'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()


class TestPromoteBeforeFetch(unittest.TestCase):
    """Verify promote_approved_sites is called before data fetch."""

    @patch('main._run_feature_server_sync')
    def test_response_includes_pending_sites_block(self, mock_sync):
        """Sync response should include pending_sites metadata."""
        mock_sync.return_value = {
            'status': 'success',
            'pending_sites': {
                'new_pending': 0,
                'total_pending': 2,
                'promoted': 1,
                'coordinate_matched': 0,
                'names': [],
            }
        }
        result = mock_sync()
        self.assertIn('pending_sites', result)
        self.assertIn('total_pending', result['pending_sites'])


if __name__ == '__main__':
    unittest.main()
```

Note: Full integration testing of `main.py` is complex due to GCS mocking. The key changes are straightforward orchestration wiring. Focus testing on `site_manager.py` and `chemical_processor.py` where the logic lives.

**Step 2: Modify `_run_feature_server_sync` in `main.py`**

Three changes:

**a) Import site_manager (after existing imports, ~line 161):**

```python
from site_manager import promote_approved_sites, get_pending_site_summary
```

**b) Add promotion call after DB download, before fetch (~line 157):**

```python
            # Promote any approved pending sites before fetching new data
            promote_conn = sqlite3.connect(temp_db.name)
            try:
                promote_result = promote_approved_sites(promote_conn)
                if promote_result['promoted'] > 0:
                    logger.info(
                        f"Promoted {promote_result['promoted']} approved sites: "
                        f"{promote_result['names']}"
                    )
            finally:
                promote_conn.close()
```

**c) Add pending_sites block to response (after `site_classification`, ~line 287):**

```python
    # Add pending sites info to response
    pending_conn = sqlite3.connect(temp_db.name)
    try:
        pending_summary = get_pending_site_summary(pending_conn)
    finally:
        pending_conn.close()

    result['pending_sites'] = {
        'new_pending': len(insert_result.get('new_pending', [])),
        'total_pending': pending_summary['total_pending'],
        'promoted': promote_result.get('promoted', 0),
        'coordinate_matched': insert_result.get('coordinate_matched', 0),
        'names': insert_result.get('new_pending', []),
    }
```

Also add pending info to the `update_sync_timestamp` metadata.

**Step 3: Run tests**

Run: `pytest tests/survey123_sync/ -v`
Expected: All PASS.

**Step 4: Run full test suite**

Run: `pytest -v`
Expected: All pass.

**Step 5: Commit**

```bash
git add cloud_functions/survey123_sync/main.py tests/survey123_sync/test_main_pending_sites.py
git commit -m "feat: add pending site orchestration to cloud sync"
```

---

### Task 8: Ensure `pending_sites` table is created in cloud function context

The cloud function downloads an existing DB from GCS. It won't have the `pending_sites` table until the next `reset_database` bakes one into GCS. We need to ensure the table is created if it doesn't exist.

**Files:**
- Modify: `cloud_functions/survey123_sync/chemical_processor.py` (add table creation in `insert_processed_data_to_db`)

**Step 1: Add CREATE TABLE IF NOT EXISTS at the start of `insert_processed_data_to_db`**

After the existing `CREATE UNIQUE INDEX` statement (line ~96), add:

```python
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_sites (
                pending_site_id INTEGER PRIMARY KEY,
                site_name TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                first_seen_date TEXT NOT NULL,
                source TEXT DEFAULT 'feature_server',
                status TEXT DEFAULT 'pending',
                reviewed_date TEXT,
                notes TEXT,
                nearest_site_name TEXT,
                nearest_site_distance_m REAL,
                UNIQUE(site_name)
            )
        ''')
```

This is idempotent (`IF NOT EXISTS`) and ensures the table exists regardless of when the GCS database was last rebuilt.

**Step 2: Run tests**

Run: `pytest tests/survey123_sync/ -v`
Expected: All PASS.

**Step 3: Commit**

```bash
git add cloud_functions/survey123_sync/chemical_processor.py
git commit -m "feat: ensure pending_sites table exists in cloud function context"
```

---

## Verification Checklist

After all tasks are complete:

1. `pytest -v` — full test suite passes with no regressions
2. Manual check: review `_fetch_features_paginated` sends `returnGeometry=true`
3. Manual check: `site_manager.py` imports `haversine_m` from `merge_sites`
4. Manual check: `format_to_database_schema` preserves lat/lon when present
5. Manual check: `main.py` calls `promote_approved_sites` before fetch
6. Manual check: response includes `pending_sites` block

## Pre-merge prerequisite (manual, not part of this plan)

Run `reset_database` locally with latest CSV data and upload fresh DB to GCS. This clears the backlog of 4 unknown sites so no backfill logic is needed.
