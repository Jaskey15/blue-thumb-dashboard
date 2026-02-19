"""
Tests for Survey123 data processing functions.
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
from main import process_survey123_data


class TestSurvey123DataProcessing(unittest.TestCase):
    """Test Survey123 data processing wrapper functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Sample Survey123 data
        self.sample_survey123_data = pd.DataFrame({
            'Site Name': ['Test Site 1'],
            'Sampling Date': ['5/15/2023, 10:30 AM'],
            '% Oxygen Saturation': [95.5],
            'pH #1': [7.2],
            'pH #2': [7.5],
            'Nitrate #1': [0.5],
            'Nitrate #2': [0.6],
            'Nitrite #1': [0.05],
            'Nitrite #2': [0.04],
            'Ammonia Nitrogen Range Selection': ['Low'],
            'Ammonia Nitrogen Low Reading #1': [0.1],
            'Ammonia Nitrogen Low Reading #2': [0.12],
            'Orthophosphate Range Selection': ['Low'],
            'Orthophosphate_Low1_Final': [0.02],
            'Orthophosphate_Low2_Final': [0.03],
            'Chloride Range Selection': ['Low'],
            'Chloride_Low1_Final': [25.0],
            'Chloride_Low2_Final': [26.0]
        })
    
    def test_process_survey123_data_function(self):
        """Test the data processing function wrapper."""
        # Test with valid data
        result = process_survey123_data(self.sample_survey123_data.copy())
        
        # Should process successfully
        self.assertFalse(result.empty)
        self.assertIn('Site_Name', result.columns)
        
        # Test with empty data
        empty_result = process_survey123_data(pd.DataFrame())
        self.assertTrue(empty_result.empty)
    
    def test_process_survey123_data_error_handling(self):
        """Test error handling in data processing function."""
        # Create invalid data that should cause processing errors
        invalid_df = pd.DataFrame({'Invalid': ['Data']})
        
        result = process_survey123_data(invalid_df)
        
        # Should handle errors gracefully and return empty DataFrame
        self.assertTrue(result.empty)


class TestSyncModeBehavior(unittest.TestCase):
    """Executable proof for sync mode selection and feature_server routing."""

    def test_get_sync_mode_precedence(self):
        request = MagicMock()
        request.args = {'mode': 'feature_server'}
        request.get_json.return_value = {'mode': 'survey123'}

        with patch.dict(os.environ, {'SYNC_MODE': 'survey123'}, clear=False):
            self.assertEqual(main._get_sync_mode(request), 'feature_server')

        request.args = None
        request.get_json.return_value = {'mode': 'feature_server'}
        with patch.dict(os.environ, {'SYNC_MODE': 'survey123'}, clear=False):
            self.assertEqual(main._get_sync_mode(request), 'feature_server')

        request.get_json.return_value = None
        with patch.dict(os.environ, {'SYNC_MODE': 'feature_server'}, clear=False):
            self.assertEqual(main._get_sync_mode(request), 'feature_server')

        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(main._get_sync_mode(request), 'survey123')

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
             patch('data_processing.arcgis_sync.translate_to_pipeline_schema', return_value=pd.DataFrame(records)), \
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