"""Tests for pending site orchestration in main.py."""
import os
import sqlite3
import sys
import unittest
from unittest.mock import MagicMock, patch

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'cloud_functions', 'survey123_sync'))

sys.modules['functions_framework'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()


def _seed_test_db(path):
    """Create a minimal DB so the function doesn't crash on missing tables."""
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS sites (
        site_id INTEGER PRIMARY KEY, site_name TEXT UNIQUE,
        latitude REAL, longitude REAL, active BOOLEAN DEFAULT 1,
        last_chemical_reading_date TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS pending_sites (
        pending_site_id INTEGER PRIMARY KEY, site_name TEXT NOT NULL,
        latitude REAL, longitude REAL, first_seen_date TEXT NOT NULL,
        source TEXT, status TEXT, reviewed_date TEXT, notes TEXT,
        nearest_site_name TEXT, nearest_site_distance_m REAL, UNIQUE(site_name))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS chemical_collection_events (
        event_id INTEGER PRIMARY KEY, site_id INTEGER, sample_id INTEGER,
        collection_date TEXT, year INTEGER, month INTEGER)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS chemical_measurements (
        event_id INTEGER, parameter_id INTEGER, value REAL, status TEXT,
        PRIMARY KEY (event_id, parameter_id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS chemical_parameters (
        parameter_id INTEGER PRIMARY KEY, parameter_code TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS chemical_reference_values (
        id INTEGER PRIMARY KEY, parameter_id INTEGER,
        threshold_type TEXT, value REAL)''')
    conn.commit()
    conn.close()
    return True


class TestPendingSitesOrchestration(unittest.TestCase):
    """Verify pending_sites lifecycle is wired into the sync pipeline."""

    def test_promote_called_before_data_insertion(self):
        """promote_approved_sites should run before insert_processed_data_to_db."""
        from datetime import datetime
        import pandas as pd

        call_order = []

        mock_db_manager = MagicMock()
        mock_db_manager.download_database.side_effect = lambda path: _seed_test_db(path)
        mock_db_manager.bucket = MagicMock()
        mock_db_manager.bucket.blob.return_value.exists.return_value = False
        mock_db_manager.upload_database.return_value = True
        mock_db_manager.update_sync_timestamp.return_value = True

        orig_promote = None
        orig_insert = None

        def track_promote(conn):
            call_order.append('promote')
            # Actually run table creation (main.py does CREATE TABLE before this)
            # but we just need to track the call order
            return {'promoted': 0, 'names': []}

        def track_insert(df, db_path):
            call_order.append('insert')
            return {'records_inserted': 0}

        with patch('site_manager.promote_approved_sites', side_effect=track_promote), \
             patch('site_manager.get_pending_site_summary', return_value={'total_pending': 0}), \
             patch('chemical_processor.insert_processed_data_to_db', side_effect=track_insert), \
             patch('chemical_processor.classify_active_sites_in_db', return_value={'active_count': 0, 'historic_count': 0}):

            import data_processing.arcgis_sync as arcgis_sync
            with patch.object(arcgis_sync, 'fetch_features_since', return_value=[{'objectid': 1}]), \
                 patch.object(arcgis_sync, 'translate_to_pipeline_schema', return_value=pd.DataFrame({'col': [1]})), \
                 patch.object(arcgis_sync, 'process_fetched_data', return_value=pd.DataFrame({'col': [1]})):

                from main import _run_feature_server_sync
                result = _run_feature_server_sync(mock_db_manager, datetime.now())

        self.assertEqual(call_order, ['promote', 'insert'])
        self.assertIn('pending_sites', result)


if __name__ == '__main__':
    unittest.main()
