"""Tests for site registration in the sync pipeline."""
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


class TestSiteRegistration(unittest.TestCase):
    """Verify site registration is wired into the sync pipeline."""

    def test_new_sites_created_in_response(self):
        """Sync pipeline runs successfully and reports new_sites_created when present."""
        from datetime import datetime
        import pandas as pd

        mock_db_manager = MagicMock()
        mock_db_manager.download_database.side_effect = lambda path: _seed_test_db(path)
        mock_db_manager.bucket = MagicMock()
        mock_db_manager.bucket.blob.return_value.exists.return_value = False
        mock_db_manager.upload_database.return_value = True
        mock_db_manager.update_sync_timestamp.return_value = True

        with patch('chemical_processor.insert_processed_data_to_db',
                   return_value={'records_inserted': 2, 'new_sites_created': 1}), \
             patch('chemical_processor.classify_active_sites_in_db',
                   return_value={'active_count': 1, 'historic_count': 0, 'sites_classified': 1}):

            import data_processing.arcgis_sync as arcgis_sync
            with patch.object(arcgis_sync, 'fetch_features_since', return_value=[{'objectid': 1}]), \
                 patch.object(arcgis_sync, 'translate_to_pipeline_schema', return_value=pd.DataFrame({'col': [1]})), \
                 patch.object(arcgis_sync, 'process_fetched_data', return_value=pd.DataFrame({'col': [1]})):

                from main import _run_feature_server_sync
                result = _run_feature_server_sync(mock_db_manager, datetime.now())

        self.assertIsInstance(result, dict)
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result.get('new_sites_created'), 1)


if __name__ == '__main__':
    unittest.main()
