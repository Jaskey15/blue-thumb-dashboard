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
            cursor.execute("DELETE FROM pending_sites WHERE site_name = 'Test Creek'")
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
