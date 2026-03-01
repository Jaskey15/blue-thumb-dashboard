"""
Tests for chemical processing adapter functionality.
"""

import os
import sqlite3
import sys
import tempfile
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

from chemical_processor import (
    classify_active_sites_in_db,
    get_reference_values_from_db,
    insert_processed_data_to_db,
)


class TestChemicalProcessor(unittest.TestCase):
    """Test chemical processing adapter functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sample_reference_values = {
            'do_percent': {'normal min': 80, 'normal max': 130},
            'pH': {'normal min': 6.5, 'normal max': 9.0},
            'soluble_nitrogen': {'normal': 0.8, 'caution': 1.5},
            'Phosphorus': {'normal': 0.05, 'caution': 0.1},
            'Chloride': {'poor': 250}
        }
    
    @patch('chemical_processor.pd.read_sql_query')
    def test_get_reference_values_from_db_success(self, mock_read_sql):
        """Test successful retrieval of reference values from database."""
        # Mock database query result
        mock_read_sql.return_value = pd.DataFrame({
            'parameter_code': ['do_percent', 'do_percent', 'pH', 'pH'],
            'threshold_type': ['normal_min', 'normal_max', 'normal_min', 'normal_max'],
            'value': [80, 130, 6.5, 9.0]
        })
        
        # Mock database connection
        mock_conn = MagicMock()
        
        result = get_reference_values_from_db(mock_conn)
        
        # Verify structure
        self.assertIn('do_percent', result)
        self.assertIn('pH', result)
        self.assertEqual(result['do_percent']['normal min'], 80)
        self.assertEqual(result['do_percent']['normal max'], 130)
        self.assertEqual(result['pH']['normal min'], 6.5)
        self.assertEqual(result['pH']['normal max'], 9.0)
    
    @patch('chemical_processor.pd.read_sql_query')
    def test_get_reference_values_from_db_empty(self, mock_read_sql):
        """Test handling of empty reference values."""
        # Mock empty query result
        mock_read_sql.return_value = pd.DataFrame()
        
        mock_conn = MagicMock()
        
        with self.assertRaises(Exception) as context:
            get_reference_values_from_db(mock_conn)
        
        self.assertIn("No chemical reference values found", str(context.exception))
    
    @patch('chemical_processor.pd.read_sql_query')
    def test_get_reference_values_from_db_error(self, mock_read_sql):
        """Test handling of database errors."""
        # Mock database error
        mock_read_sql.side_effect = Exception("Database connection failed")
        
        mock_conn = MagicMock()
        
        with self.assertRaises(Exception) as context:
            get_reference_values_from_db(mock_conn)
        
        self.assertIn("Cannot retrieve chemical reference values", str(context.exception))
    
    @patch('chemical_processor.sqlite3.connect')
    @patch('chemical_processor.get_reference_values_from_db')
    def test_insert_processed_data_to_db_success(self, mock_get_ref, mock_connect):
        """Test successful database insertion."""
        # Mock processed data
        processed_data = pd.DataFrame({
            'Site_Name': ['Test Site 1'],
            'Date': [pd.Timestamp('2023-01-15')],
            'Year': [2023],
            'Month': [1],
            'do_percent': [95.5],
            'pH': [7.2],
            'soluble_nitrogen': [0.8],
            'Phosphorus': [0.05],
            'Chloride': [25.0]
        })
        
        # Mock database connection and operations
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock reference values
        mock_get_ref.return_value = self.sample_reference_values
        
        # Mock site lookup query
        site_df = pd.DataFrame({
            'site_id': [1],
            'site_name': ['Test Site 1']
        })
        with patch('chemical_processor.pd.read_sql_query', return_value=site_df):
            # Mock event ID retrieval
            mock_cursor.fetchone.return_value = (123,)  # event_id
            
            result = insert_processed_data_to_db(processed_data, '/fake/path/db.sqlite')
        
        # Verify success
        self.assertIn('records_inserted', result)
        self.assertGreater(result['records_inserted'], 0)
        
        # Verify database operations were called
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('chemical_processor.sqlite3.connect')
    def test_insert_processed_data_to_db_empty_data(self, mock_connect):
        """Test handling of empty data insertion."""
        empty_df = pd.DataFrame()
        
        result = insert_processed_data_to_db(empty_df, '/fake/path/db.sqlite')
        
        # Should return appropriate result for empty data
        self.assertEqual(result['records_inserted'], 0)
        self.assertIn('error', result)
        self.assertEqual(result['error'], 'No data to insert')
        
        # Should not attempt database connection
        mock_connect.assert_not_called()
    
    @patch('chemical_processor.sqlite3.connect')
    @patch('chemical_processor.get_reference_values_from_db')
    def test_insert_processed_data_to_db_site_not_found(self, mock_get_ref, mock_connect):
        """Test handling when site is not found in database."""
        # Mock processed data with unknown site
        processed_data = pd.DataFrame({
            'Site_Name': ['Unknown Site'],
            'Date': [pd.Timestamp('2023-01-15')],
            'do_percent': [95.5]
        })
        
        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock reference values
        mock_get_ref.return_value = self.sample_reference_values
        
        # Mock empty site lookup (site not found)
        empty_site_df = pd.DataFrame(columns=['site_id', 'site_name'])
        with patch('chemical_processor.pd.read_sql_query', return_value=empty_site_df):
            result = insert_processed_data_to_db(processed_data, '/fake/path/db.sqlite')
        
        # Should still succeed but insert 0 records
        self.assertEqual(result['records_inserted'], 0)
    
    @patch('chemical_processor.sqlite3.connect')
    def test_insert_processed_data_to_db_database_error(self, mock_connect):
        """Test handling of database connection errors."""
        # Mock database connection error
        mock_connect.side_effect = Exception("Database connection failed")
        
        processed_data = pd.DataFrame({
            'Site_Name': ['Test Site'],
            'Date': [pd.Timestamp('2023-01-15')],
            'do_percent': [95.5]
        })
        
        result = insert_processed_data_to_db(processed_data, '/fake/path/db.sqlite')
        
        # Should return error result
        self.assertEqual(result['records_inserted'], 0)
        self.assertIn('error', result)
        self.assertIn('Database connection failed', result['error'])

    def test_classify_active_sites_in_db_success(self):
        """Test successful site classification."""
        # Create a temporary in-memory database
        with tempfile.NamedTemporaryFile(suffix='.db') as temp_db:
            # Setup test database
            conn = sqlite3.connect(temp_db.name)
            cursor = conn.cursor()
            
            # Create minimal required tables
            cursor.execute('''
                CREATE TABLE sites (
                    site_id INTEGER PRIMARY KEY,
                    site_name TEXT NOT NULL,
                    active BOOLEAN DEFAULT 1,
                    last_chemical_reading_date TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE chemical_collection_events (
                    event_id INTEGER PRIMARY KEY,
                    site_id INTEGER NOT NULL,
                    collection_date TEXT NOT NULL,
                    FOREIGN KEY (site_id) REFERENCES sites (site_id)
                )
            ''')
            
            # Insert test data
            cursor.execute("INSERT INTO sites (site_id, site_name) VALUES (1, 'Active Site')")
            cursor.execute("INSERT INTO sites (site_id, site_name) VALUES (2, 'Historic Site')")
            
            # Add recent data for active site (within 1 year)
            cursor.execute("INSERT INTO chemical_collection_events (site_id, collection_date) VALUES (1, '2023-12-01')")
            
            # Add old data for historic site (more than 1 year old)
            cursor.execute("INSERT INTO chemical_collection_events (site_id, collection_date) VALUES (2, '2020-01-01')")
            
            conn.commit()
            conn.close()
            
            # Test the classification function
            result = classify_active_sites_in_db(temp_db.name)
            
            # Verify results
            self.assertNotIn('error', result)
            self.assertEqual(result['sites_classified'], 2)
            self.assertEqual(result['active_count'], 1)
            self.assertEqual(result['historic_count'], 1)
            self.assertIn('cutoff_date', result)
            self.assertIn('most_recent_date', result)
    
    def test_classify_active_sites_in_db_no_chemical_data(self):
        """Test site classification with no chemical data."""
        # Create a temporary in-memory database
        with tempfile.NamedTemporaryFile(suffix='.db') as temp_db:
            # Setup test database with sites but no chemical data
            conn = sqlite3.connect(temp_db.name)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE sites (
                    site_id INTEGER PRIMARY KEY,
                    site_name TEXT NOT NULL,
                    active BOOLEAN DEFAULT 1,
                    last_chemical_reading_date TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE chemical_collection_events (
                    event_id INTEGER PRIMARY KEY,
                    site_id INTEGER NOT NULL,
                    collection_date TEXT NOT NULL,
                    FOREIGN KEY (site_id) REFERENCES sites (site_id)
                )
            ''')
            
            cursor.execute("INSERT INTO sites (site_id, site_name) VALUES (1, 'Test Site')")
            
            conn.commit()
            conn.close()
            
            # Test the classification function
            result = classify_active_sites_in_db(temp_db.name)
            
            # Should return error for no chemical data
            self.assertIn('error', result)
            self.assertEqual(result['sites_classified'], 0)
    
    def test_classify_active_sites_in_db_database_error(self):
        """Test handling of database errors during classification."""
        # Test with non-existent database file
        result = classify_active_sites_in_db('/non/existent/path/test.db')
        
        # Should return error result
        self.assertIn('error', result)
        self.assertEqual(result['sites_classified'], 0)


if __name__ == '__main__':
    unittest.main() 