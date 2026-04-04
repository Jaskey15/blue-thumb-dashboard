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

    def test_coordinate_update_on_conflict(self):
        """Second insert with coordinates should update a pending site that had None coords."""
        from site_manager import resolve_unknown_site

        existing = self._get_existing_sites()
        # First: no coordinates
        resolve_unknown_site('New Creek', None, None, existing, self.conn)
        # Second: with coordinates
        resolve_unknown_site('New Creek', 36.5, -96.5, existing, self.conn)

        cursor = self.conn.cursor()
        cursor.execute("SELECT latitude, longitude FROM pending_sites WHERE site_name = 'New Creek'")
        row = cursor.fetchone()
        self.assertAlmostEqual(row[0], 36.5)
        self.assertAlmostEqual(row[1], -96.5)


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
