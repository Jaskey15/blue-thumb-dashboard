"""
Tests for FeatureServer sync behavior.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'cloud_functions', 'survey123_sync'))

# Mock Cloud Function specific modules before importing
sys.modules['functions_framework'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()

import main


class TestSyncModeBehavior(unittest.TestCase):
    """Executable proof for feature_server sync routing."""

    @patch('main.os.unlink')
    @patch('main.tempfile.NamedTemporaryFile')
    @patch('main.datetime')
    def test_run_feature_server_sync_no_records_updates_metadata(self, mock_datetime, mock_temp_file, mock_unlink):
        start_time = pd.Timestamp('2026-02-18T00:00:00').to_pydatetime()
        mock_datetime.now.return_value = start_time

        temp_ctx = MagicMock()
        temp_ctx.__enter__.return_value.name = '/tmp/test-sync.db'
        temp_ctx.__exit__.return_value = False
        mock_temp_file.return_value = temp_ctx

        db_manager = MagicMock()
        db_manager.download_database.return_value = True
        db_manager.bucket.blob.return_value.exists.return_value = False

        with patch('main._get_db_latest_chemical_date', return_value='2020-01-01'), \
             patch('data_processing.arcgis_sync.fetch_features_since', return_value=[]):
            result = main._run_feature_server_sync(db_manager, start_time)

        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['mode'], 'feature_server')
        self.assertEqual(result['records_fetched'], 0)
        self.assertEqual(result['sync_strategy'], 'day')
        db_manager.update_sync_timestamp.assert_called_once()
        mock_unlink.assert_called_once_with('/tmp/test-sync.db')

    @patch('main.os.unlink')
    @patch('main.tempfile.NamedTemporaryFile')
    @patch('main.datetime')
    def test_run_feature_server_sync_processes_and_uploads(self, mock_datetime, mock_temp_file, mock_unlink):
        start_time = pd.Timestamp('2026-02-18T00:00:00').to_pydatetime()
        mock_datetime.now.return_value = start_time

        temp_ctx = MagicMock()
        temp_ctx.__enter__.return_value.name = '/tmp/test-sync.db'
        temp_ctx.__exit__.return_value = False
        mock_temp_file.return_value = temp_ctx

        db_manager = MagicMock()
        db_manager.download_database.return_value = True
        db_manager.upload_database.return_value = True
        db_manager.bucket.blob.return_value.exists.return_value = True
        db_manager.get_last_sync_timestamp.return_value = start_time

        records = [{'objectid': 1}]
        processed = pd.DataFrame([{'sample_id': 1, 'Site_Name': 'A'}])

        with patch('data_processing.arcgis_sync.fetch_features_edited_since', return_value=records), \
             patch('data_processing.arcgis_sync.prepare_dataframe', return_value=pd.DataFrame(records)), \
             patch('data_processing.arcgis_sync.process_fetched_data', return_value=processed), \
             patch('chemical_processor.insert_processed_data_to_db', return_value={'records_inserted': 1}), \
             patch('chemical_processor.classify_active_sites_in_db', return_value={'sites_classified': 1, 'active_count': 1, 'historic_count': 0}):
            result = main._run_feature_server_sync(db_manager, start_time)

        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['mode'], 'feature_server')
        self.assertEqual(result['records_fetched'], 1)
        self.assertEqual(result['records_processed'], 1)
        self.assertEqual(result['records_inserted'], 1)
        self.assertEqual(result['sync_strategy'], 'editdate')
        self.assertIn('site_classification', result)
        db_manager.upload_database.assert_called_once_with('/tmp/test-sync.db')
        db_manager.update_sync_timestamp.assert_called_once()
        mock_unlink.assert_called_once_with('/tmp/test-sync.db')


if __name__ == '__main__':
    unittest.main() 