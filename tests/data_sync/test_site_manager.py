"""Tests for site_manager: resolve unknown sites via name, alias, coords, or auto-insert."""
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import MagicMock

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'cloud_functions', 'data_sync'))

# Mock Cloud Function specific modules
sys.modules['functions_framework'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()


def _create_test_db(path):
    """Create a minimal test database with sites table."""
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

    def test_no_match_auto_inserts_site(self):
        """Site far from all existing should be auto-inserted into sites and return a valid site_id."""
        from site_manager import resolve_unknown_site

        existing = self._get_existing_sites()
        result = resolve_unknown_site(
            'New Creek: Remote', 36.0, -96.0, existing, self.conn
        )
        self.assertIsNotNone(result)
        self.assertIsInstance(result, int)

        cursor = self.conn.cursor()
        cursor.execute("SELECT site_name, active FROM sites WHERE site_name = 'New Creek: Remote'")
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[1], 1)  # active

    def test_duplicate_auto_insert_ignored(self):
        """Second call for same site_name should not create a duplicate, returns same site_id."""
        from site_manager import resolve_unknown_site

        existing = self._get_existing_sites()
        result1 = resolve_unknown_site('New Creek', 36.0, -96.0, existing, self.conn)
        result2 = resolve_unknown_site('New Creek', 36.0, -96.0, existing, self.conn)

        self.assertEqual(result1, result2)

        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sites WHERE site_name = 'New Creek'")
        self.assertEqual(cursor.fetchone()[0], 1)

    def test_no_coordinates_auto_inserts_site(self):
        """Site with None coordinates should be auto-inserted into sites and return a valid site_id."""
        from site_manager import resolve_unknown_site

        existing = self._get_existing_sites()
        result = resolve_unknown_site('No Coords Creek', None, None, existing, self.conn)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, int)

        cursor = self.conn.cursor()
        cursor.execute("SELECT site_name, latitude, longitude FROM sites WHERE site_name = 'No Coords Creek'")
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row[1])  # latitude is NULL
        self.assertIsNone(row[2])  # longitude is NULL

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


if __name__ == '__main__':
    unittest.main()
