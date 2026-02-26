"""
Tests for database management functionality in Cloud Storage.
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'cloud_functions', 'survey123_sync'))

# Mock Cloud Function specific modules before importing
sys.modules['functions_framework'] = MagicMock()
sys.modules['google.cloud'] = MagicMock() 
sys.modules['google.cloud.storage'] = MagicMock()

from main import DatabaseManager


class TestDatabaseManager(unittest.TestCase):
    """Test database management logic for Cloud Storage operations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.bucket_name = "test-bucket"
        
        # Mock Google Cloud Storage components
        with patch('main.storage.Client') as mock_storage_client:
            self.mock_client = MagicMock()
            self.mock_bucket = MagicMock()
            self.mock_blob = MagicMock()
            
            mock_storage_client.return_value = self.mock_client
            self.mock_client.bucket.return_value = self.mock_bucket
            self.mock_bucket.blob.return_value = self.mock_blob
            
            self.db_manager = DatabaseManager(self.bucket_name)
    
    def test_download_database_success(self):
        """Test successful database download."""
        with tempfile.NamedTemporaryFile() as temp_file:
            # Mock blob exists and download succeeds
            self.mock_blob.exists.return_value = True
            self.mock_blob.download_to_filename.return_value = None
            
            result = self.db_manager.download_database(temp_file.name)
            
            # Verify success
            self.assertTrue(result)
            self.mock_blob.download_to_filename.assert_called_once_with(temp_file.name)
            self.mock_bucket.blob.assert_called_with('blue_thumb.db')
    
    def test_download_database_not_found(self):
        """Test handling when database doesn't exist."""
        with tempfile.NamedTemporaryFile() as temp_file:
            # Mock blob doesn't exist
            self.mock_blob.exists.return_value = False
            
            result = self.db_manager.download_database(temp_file.name)
            
            # Should return False
            self.assertFalse(result)
            self.mock_blob.download_to_filename.assert_not_called()
    
    def test_download_database_error(self):
        """Test handling of download errors."""
        with tempfile.NamedTemporaryFile() as temp_file:
            # Mock blob exists but download fails
            self.mock_blob.exists.return_value = True
            self.mock_blob.download_to_filename.side_effect = Exception("Download failed")
            
            result = self.db_manager.download_database(temp_file.name)
            
            # Should return False on error
            self.assertFalse(result)
    
    def test_upload_database_success_with_backup(self):
        """Test successful database upload with backup creation."""
        with tempfile.NamedTemporaryFile() as temp_file:
            # Mock existing blob for backup
            self.mock_blob.exists.return_value = True
            self.mock_blob.download_as_string.return_value = b"existing_db_content"
            
            # Mock backup blob
            mock_backup_blob = MagicMock()
            self.mock_bucket.blob.side_effect = [self.mock_blob, mock_backup_blob, self.mock_blob]
            
            result = self.db_manager.upload_database(temp_file.name)
            
            # Verify success
            self.assertTrue(result)
            
            # Verify backup was created
            mock_backup_blob.upload_from_string.assert_called_once_with(b"existing_db_content")
            
            # Verify main database was uploaded
            self.mock_blob.upload_from_filename.assert_called_once_with(temp_file.name)
    
    def test_upload_database_success_no_existing_db(self):
        """Test successful database upload when no existing database."""
        with tempfile.NamedTemporaryFile() as temp_file:
            # Mock no existing blob
            self.mock_blob.exists.return_value = False
            
            result = self.db_manager.upload_database(temp_file.name)
            
            # Verify success
            self.assertTrue(result)
            
            # Verify no backup attempt (since no existing database)
            self.mock_blob.download_as_string.assert_not_called()
            
            # Verify main database was uploaded
            self.mock_blob.upload_from_filename.assert_called_once_with(temp_file.name)
    
    def test_upload_database_error(self):
        """Test handling of upload errors."""
        with tempfile.NamedTemporaryFile() as temp_file:
            # Mock upload failure
            self.mock_blob.exists.return_value = False
            self.mock_blob.upload_from_filename.side_effect = Exception("Upload failed")
            
            result = self.db_manager.upload_database(temp_file.name)
            
            # Should return False on error
            self.assertFalse(result)
    
    def test_get_last_sync_timestamp_exists(self):
        """Test getting last sync timestamp when metadata exists."""
        # Mock metadata blob exists
        mock_metadata_blob = MagicMock()
        mock_metadata_blob.exists.return_value = True
        
        test_timestamp = datetime(2023, 6, 15, 10, 30, 0)
        metadata = {
            'last_sync_timestamp': test_timestamp.isoformat(),
            'last_sync_status': 'success'
        }
        mock_metadata_blob.download_as_string.return_value = json.dumps(metadata)
        
        self.mock_bucket.blob.return_value = mock_metadata_blob
        
        result = self.db_manager.get_last_sync_timestamp('sync_metadata/last_feature_server_sync.json')

        # Should return parsed timestamp
        self.assertEqual(result, test_timestamp)
        self.mock_bucket.blob.assert_called_with('sync_metadata/last_feature_server_sync.json')
    
    def test_get_last_sync_timestamp_not_exists(self):
        """Test getting last sync timestamp when metadata doesn't exist."""
        # Mock metadata blob doesn't exist
        mock_metadata_blob = MagicMock()
        mock_metadata_blob.exists.return_value = False
        
        self.mock_bucket.blob.return_value = mock_metadata_blob
        
        result = self.db_manager.get_last_sync_timestamp('sync_metadata/last_feature_server_sync.json')

        # Should return default timestamp (7 days ago)
        expected_cutoff = datetime.now() - timedelta(days=7)
        self.assertAlmostEqual(result, expected_cutoff, delta=timedelta(seconds=10))

    def test_get_last_sync_timestamp_error(self):
        """Test handling of errors when reading sync timestamp."""
        # Mock metadata blob exists but has invalid JSON
        mock_metadata_blob = MagicMock()
        mock_metadata_blob.exists.return_value = True
        mock_metadata_blob.download_as_string.return_value = "invalid json"

        self.mock_bucket.blob.return_value = mock_metadata_blob

        result = self.db_manager.get_last_sync_timestamp('sync_metadata/last_feature_server_sync.json')
        
        # Should return default timestamp on error
        expected_cutoff = datetime.now() - timedelta(days=7)
        self.assertAlmostEqual(result, expected_cutoff, delta=timedelta(seconds=10))
    
    def test_update_sync_timestamp_success(self):
        """Test successful sync timestamp update."""
        test_timestamp = datetime(2023, 6, 15, 14, 45, 0)
        
        # Mock metadata blob
        mock_metadata_blob = MagicMock()
        self.mock_bucket.blob.return_value = mock_metadata_blob
        
        result = self.db_manager.update_sync_timestamp(test_timestamp, metadata_blob_name='sync_metadata/last_feature_server_sync.json')

        # Verify success
        self.assertTrue(result)

        # Verify metadata was uploaded
        expected_metadata = {
            'last_sync_timestamp': test_timestamp.isoformat(),
            'last_sync_status': 'success'
        }
        mock_metadata_blob.upload_from_string.assert_called_once_with(
            json.dumps(expected_metadata)
        )
        self.mock_bucket.blob.assert_called_with('sync_metadata/last_feature_server_sync.json')
    
    def test_update_sync_timestamp_error(self):
        """Test handling of errors when updating sync timestamp."""
        test_timestamp = datetime(2023, 6, 15, 14, 45, 0)
        
        # Mock upload failure
        mock_metadata_blob = MagicMock()
        mock_metadata_blob.upload_from_string.side_effect = Exception("Upload failed")
        self.mock_bucket.blob.return_value = mock_metadata_blob
        
        result = self.db_manager.update_sync_timestamp(test_timestamp, metadata_blob_name='sync_metadata/last_feature_server_sync.json')

        # Should return False on error
        self.assertFalse(result)
    
    def test_backup_filename_format(self):
        """Test that backup filenames follow expected format."""
        with tempfile.NamedTemporaryFile() as temp_file:
            # Mock existing blob for backup
            self.mock_blob.exists.return_value = True
            self.mock_blob.download_as_string.return_value = b"test_content"
            
            # Capture calls to blob() to check backup filename
            blob_calls = []
            
            def mock_blob_side_effect(blob_name):
                blob_calls.append(blob_name)
                if 'backups/' in blob_name:
                    return MagicMock()  # backup blob
                return self.mock_blob  # main blob
            
            self.mock_bucket.blob.side_effect = mock_blob_side_effect
            
            self.db_manager.upload_database(temp_file.name)
            
            # Find the backup call
            backup_calls = [call for call in blob_calls if call.startswith('backups/')]
            self.assertEqual(len(backup_calls), 1)
            
            # Check backup filename format
            backup_filename = backup_calls[0]
            self.assertTrue(backup_filename.startswith('backups/blue_thumb_backup_'))
            self.assertTrue(backup_filename.endswith('.db'))


if __name__ == '__main__':
    unittest.main() 