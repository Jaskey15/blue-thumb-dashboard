"""
test_reset.py - Tests for database reset functionality.
Tests the complete database reset process including file deletion, schema recreation, and data reloading.
"""

import os
import sqlite3
from unittest.mock import patch

from database.reset_database import (
    delete_database_file,
    recreate_schema,
    reload_all_data,
    reset_database,
)


class TestDatabaseDeletion:
    """Test database file deletion functionality."""
    
    def test_delete_existing_database(self, temp_db):
        """Test deleting an existing database file."""
        # Get the database path
        cursor = temp_db.cursor()
        db_path = cursor.execute("PRAGMA database_list").fetchone()[2]
        temp_db.close()
        
        # Verify file exists
        assert os.path.exists(db_path)
        
        # Delete database
        result = delete_database_file()
        
        # Verify deletion
        assert result is True
        assert not os.path.exists(db_path)
    
    def test_delete_nonexistent_database(self):
        """Test attempting to delete a non-existent database file."""
        with patch('os.path.exists', return_value=False):
            result = delete_database_file()
            assert result is True  # Should return True even if file doesn't exist
    
    def test_delete_with_open_connections(self, temp_db):
        """Test deleting database with open connections."""
        # Create additional connection
        db_path = temp_db.cursor().execute("PRAGMA database_list").fetchone()[2]
        second_conn = sqlite3.connect(db_path)
        
        try:
            # Attempt deletion
            result = delete_database_file()
            
            # On Unix-like systems, we can delete files with open handles
            if os.name == 'posix':
                assert result is True
                assert not os.path.exists(db_path)
            else:
                # On Windows, this might fail due to file locking
                pass
        finally:
            second_conn.close()
            temp_db.close()
    
    @patch('os.remove')
    def test_delete_with_insufficient_permissions(self, mock_remove):
        """Test deletion with insufficient permissions."""
        mock_remove.side_effect = PermissionError("Permission denied")
        
        result = delete_database_file()
        assert result is False

class TestSchemaRecreation:
    """Test schema recreation process."""
    
    def test_successful_schema_recreation(self, temp_db):
        """Test recreating schema successfully."""
        # Get the path before closing
        db_path = temp_db.cursor().execute("PRAGMA database_list").fetchone()[2]
        temp_db.close()
        
        result = recreate_schema()
        assert result is True
        
        # Create new connection to verify schema
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check for essential tables
        tables = cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """).fetchall()
        
        assert len(tables) > 0
        assert ('sites',) in tables
        conn.close()
    
    def test_recreation_after_deletion(self, temp_db):
        """Test recreating schema after database deletion."""
        db_path = temp_db.cursor().execute("PRAGMA database_list").fetchone()[2]
        temp_db.close()
        
        # Delete and recreate
        os.remove(db_path)
        result = recreate_schema()
        
        assert result is True
        assert os.path.exists(db_path)
    
    def test_recreation_with_existing_tables(self, temp_db):
        """Test recreating schema when tables already exist."""
        result = recreate_schema()
        assert result is True
        
        # Verify tables were recreated
        cursor = temp_db.cursor()
        tables = cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """).fetchall()
        
        assert len(tables) > 0
        assert ('sites',) in tables

class TestDataReloading:
    """Test data reloading process."""
    
    @patch('database.reset_database.verify_cleaned_csvs')
    @patch('database.reset_database.consolidate_sites_from_csvs')
    @patch('database.reset_database.process_site_data')
    @patch('database.reset_database.merge_duplicate_sites')
    @patch('database.reset_database.generate_final_data_summary')
    @patch('database.reset_database.load_chemical_data_to_db')
    @patch('database.reset_database.sync_all_chemical_data')
    @patch('database.reset_database.load_fish_data')
    @patch('database.reset_database.load_macroinvertebrate_data')
    @patch('database.reset_database.load_habitat_data')
    @patch('database.reset_database.classify_active_sites')
    @patch('database.reset_database.cleanup_unused_sites')
    def test_complete_data_loading(
        self, mock_cleanup, mock_classify, mock_habitat, mock_macro, mock_fish,
        mock_sync_all, mock_chemical, mock_summary, mock_merge,
        mock_site, mock_consolidate, mock_verify
    ):
        """Test loading all data types with new Sites First approach."""
        # Set up all mocks to succeed (Phase 1: Site Unification)
        mock_verify.return_value = True
        mock_consolidate.return_value = True
        mock_site.return_value = True
        mock_merge.return_value = True
        mock_summary.return_value = {
            'sites': {'total': 100, 'active': 80, 'historic': 20},
            'chemical': {'events': 500, 'measurements': 2000},
            'biological': {'fish_events': 50, 'macro_events': 75},
            'habitat': {'assessments': 25}
        }

        # Set up monitoring data loading mocks (Phase 2)
        mock_chemical.return_value = True
        mock_sync_all.return_value = {'status': 'success', 'records_inserted': 100}
        mock_fish.return_value = True
        mock_macro.return_value = True
        mock_habitat.return_value = True
        
        # Set up final cleanup mocks (Phase 3)
        mock_classify.return_value = True
        mock_cleanup.return_value = True
        
        result = reload_all_data()
        assert result is True
        
        # Verify all steps were called in the new pipeline order
        # Phase 1: Site Unification
        mock_verify.assert_called_once()
        mock_consolidate.assert_called_once()
        mock_site.assert_called_once()
        mock_merge.assert_called_once()
        
        # Phase 2: Monitoring Data Loading
        mock_chemical.assert_called_once()
        mock_sync_all.assert_called_once()
        mock_fish.assert_called_once()
        mock_macro.assert_called_once()
        mock_habitat.assert_called_once()
        
        # Phase 3: Final Data Quality and Cleanup
        mock_classify.assert_called_once()
        mock_cleanup.assert_called_once()
        
        # Summary should be called twice (once mid-pipeline, once at end)
        assert mock_summary.call_count == 2
    
    @patch('database.reset_database.verify_cleaned_csvs')
    @patch('database.reset_database.consolidate_sites_from_csvs')
    @patch('database.reset_database.process_site_data')
    def test_site_data_loading_failure(self, mock_site, mock_consolidate, mock_verify):
        """Test handling of site data loading failure in the new pipeline."""
        # Set up early steps to succeed
        mock_verify.return_value = True
        mock_consolidate.return_value = True
        # But site processing fails
        mock_site.return_value = False
        
        result = reload_all_data()
        assert result is False
        
        # Verify the pipeline stopped at site processing
        mock_verify.assert_called_once()
        mock_consolidate.assert_called_once()
        mock_site.assert_called_once()
    
    @patch('database.reset_database.verify_cleaned_csvs')
    @patch('database.reset_database.consolidate_sites_from_csvs')
    @patch('database.reset_database.process_site_data')
    @patch('database.reset_database.merge_duplicate_sites')
    @patch('database.reset_database.generate_final_data_summary')
    @patch('database.reset_database.load_chemical_data_to_db')
    def test_chemical_data_loading_failure(self, mock_chemical, mock_summary, mock_merge, mock_site, mock_consolidate, mock_verify):
        """Test handling of chemical data loading failure in the new pipeline."""
        # Set up Phase 1 (Site Unification) to succeed
        mock_verify.return_value = True
        mock_consolidate.return_value = True
        mock_site.return_value = True
        mock_merge.return_value = True
        mock_summary.return_value = {
            'sites': {'total': 100, 'active': 80, 'historic': 20},
            'chemical': {'events': 0, 'measurements': 0},
            'biological': {'fish_events': 0, 'macro_events': 0},
            'habitat': {'assessments': 0}
        }
        
        # Chemical data loading raises an exception (this causes pipeline abort)
        mock_chemical.side_effect = Exception("Chemical data loading failed")
        
        result = reload_all_data()
        assert result is False
        
        # Verify Phase 1 completed successfully but Phase 2 failed at chemical loading
        mock_verify.assert_called_once()
        mock_consolidate.assert_called_once()
        mock_site.assert_called_once()
        mock_merge.assert_called_once()
        mock_summary.assert_called_once()  # Called once in Phase 1
        mock_chemical.assert_called_once()

class TestResetProcess:
    """Test complete reset process."""
    
    @patch('database.reset_database.delete_database_file')
    @patch('database.reset_database.recreate_schema')
    @patch('database.reset_database.reload_all_data')
    def test_successful_reset(
        self, mock_reload, mock_recreate, mock_delete
    ):
        """Test successful complete reset process."""
        # Set up all steps to succeed
        mock_delete.return_value = True
        mock_recreate.return_value = True
        mock_reload.return_value = True
        
        result = reset_database()
        assert result is True
        
        # Verify steps were called in order
        mock_delete.assert_called_once()
        mock_recreate.assert_called_once()
        mock_reload.assert_called_once()
    
    @patch('database.reset_database.delete_database_file')
    @patch('database.reset_database.recreate_schema')
    @patch('database.reset_database.reload_all_data')
    def test_partial_failure_recovery(
        self, mock_reload, mock_recreate, mock_delete
    ):
        """Test recovery from partial failures during reset."""
        # Set up schema recreation to fail
        mock_delete.return_value = True
        mock_recreate.return_value = False
        
        result = reset_database()
        assert result is False
        
        # Verify reload was not attempted after schema failure
        mock_reload.assert_not_called()
    
    @patch('database.reset_database.delete_database_file')
    @patch('database.reset_database.recreate_schema')
    @patch('database.reset_database.reload_all_data')
    def test_reset_with_active_connections(
        self, mock_reload, mock_recreate, mock_delete, temp_db
    ):
        """Test resetting database with active connections."""
        # Set up mocks to succeed
        mock_delete.return_value = True
        mock_recreate.return_value = True
        mock_reload.return_value = True
        
        # Create additional connection
        db_path = temp_db.cursor().execute("PRAGMA database_list").fetchone()[2]
        second_conn = sqlite3.connect(db_path)
        
        try:
            result = reset_database()
            assert result is True
            
            # Verify steps were called
            mock_delete.assert_called_once()
            mock_recreate.assert_called_once()
            mock_reload.assert_called_once()
        finally:
            second_conn.close()
            temp_db.close()
    
    @patch('time.time')
    @patch('database.reset_database.delete_database_file')
    @patch('database.reset_database.recreate_schema')
    @patch('database.reset_database.reload_all_data')
    def test_reset_performance(
        self, mock_reload, mock_recreate, mock_delete, mock_time
    ):
        """Test reset process performance."""
        # Set up time mock to simulate 10 second execution
        mock_time.side_effect = [0, 10]
        
        # Set up operation mocks
        mock_delete.return_value = True
        mock_recreate.return_value = True
        mock_reload.return_value = True
        
        result = reset_database()
        assert result is True 