"""
Test suite for site management functionality.
Tests site consolidation, duplicate merging, and database management.
"""

import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from data_processing import setup_logging

# Import functions from consolidate_sites.py
from data_processing.consolidate_sites import (
    consolidate_sites,
    detect_conflicts,
    extract_sites_from_csv,
    save_consolidated_data,
)

# Import functions from merge_sites.py
from data_processing.merge_sites import (
    analyze_coordinate_duplicates,
    determine_preferred_site,
    find_duplicate_coordinate_groups,
    merge_duplicate_sites,
    transfer_site_data,
)

# Import functions from site_processing.py
from data_processing.site_processing import (
    classify_active_sites,
    cleanup_unused_sites,
    insert_sites_into_db,
    load_site_data,
    process_site_data,
)

# Set up logging for tests
logger = setup_logging("test_site_management", category="testing")


class TestSiteManagement(unittest.TestCase):
    """Test comprehensive site management functionality."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary directory for test files
        self.test_dir = tempfile.mkdtemp()
        
        # Sample data for testing
        self.sample_site_data = pd.DataFrame({
            'SiteName': ['Blue Creek at Highway 9', 'Tenmile Creek at Davis', 'Red River at Bridge'],
            'Latitude': [35.1234, 34.5678, 33.9876],
            'Longitude': [-97.1234, -96.5678, -95.9876],
            'County': ['Cleveland', 'Murray', 'Bryan'],
            'RiverBasin': ['Canadian', 'Washita', 'Red'],
            'Mod_Ecoregion': ['Cross Timbers', 'Cross Timbers', 'South Central Plains']
        })
        
        self.sample_chemical_data = pd.DataFrame({
            'SiteName': ['Blue Creek at Highway 9', 'Boggy Creek at Main St', 'Tenmile Creek at Davis'],
            'Latitude': [35.1234, 34.2345, 34.5678],
            'Longitude': [-97.1234, -96.2345, -96.5678],
            'County': ['Cleveland', 'Pontotoc', 'Murray'],
            'RiverBasin': ['Canadian', 'Red', 'Washita']
        })
        
        self.sample_updated_chemical_data = pd.DataFrame({
            'Site Name': ['Blue Creek at Highway 9', 'New Site from Updated Data'],
            'lat': [35.1234, 35.5555],
            'lon': [-97.1234, -97.5555],
            'CountyName': ['Cleveland', 'Logan']
        })
        
        # Sample sites with coordinate duplicates (for merge testing)
        self.sample_sites_with_duplicates = pd.DataFrame({
            'site_id': [1, 2, 3, 4, 5],
            'site_name': ['Blue Creek Site A', 'Blue Creek Site B', 'Red River Main', 'Red River Alt', 'Unique Site'],
            'latitude': [35.1234, 35.1235, 34.5678, 34.5679, 33.9999],
            'longitude': [-97.1234, -97.1235, -96.5678, -96.5679, -95.9999],
            'county': ['Cleveland', 'Cleveland', 'Murray', 'Murray', 'Bryan'],
            'river_basin': ['Canadian', 'Canadian', 'Red', 'Red', 'Red'],
            'ecoregion': ['Cross Timbers', 'Cross Timbers', 'Plains', 'Plains', 'Plains']
        })
        
        # Sample CSV site lists for priority testing
        self.updated_chemical_sites = {'Blue Creek Site A', 'Some Other Site'}
        self.chemical_data_sites = {'Red River Main', 'Another Site'}
        
        # Sample master sites data (for site processing)
        self.sample_master_sites = pd.DataFrame({
            'site_name': ['Blue Creek at Highway 9', 'Tenmile Creek at Davis'],
            'latitude': [35.1234, 34.5678],
            'longitude': [-97.1234, -96.5678],
            'county': ['Cleveland', 'Murray'],
            'river_basin': ['Canadian', 'Washita'],
            'ecoregion': ['Cross Timbers', 'Cross Timbers']
        })

    def tearDown(self):
        """Clean up after each test method."""
        # Clean up temporary directory
        import shutil
        shutil.rmtree(self.test_dir)

    def test_extract_sites_basic_functionality(self):
        """Test basic site extraction from CSV configuration."""
        # Create a mock configuration for site_data
        config = {
            'file': 'test_site_data.csv',
            'site_column': 'SiteName',
            'lat_column': 'Latitude',
            'lon_column': 'Longitude',
            'county_column': 'County',
            'basin_column': 'RiverBasin',
            'ecoregion_column': 'Mod_Ecoregion',
            'description': 'Test site data'
        }
        
        # Mock the file loading to return our sample data
        with patch('data_processing.consolidate_sites.pd.read_csv') as mock_read_csv:
            with patch('data_processing.consolidate_sites.os.path.exists', return_value=True):
                mock_read_csv.return_value = self.sample_site_data
                
                result = extract_sites_from_csv(config)
                
                # Verify we got the expected number of sites
                self.assertEqual(len(result), 3)
                
                # Verify all expected columns are present
                expected_columns = ['site_name', 'latitude', 'longitude', 'county', 'river_basin', 'ecoregion']
                for col in expected_columns:
                    self.assertIn(col, result.columns)
                
                # Verify site names are correctly extracted
                expected_sites = ['Blue Creek at Highway 9', 'Tenmile Creek at Davis', 'Red River at Bridge']
                actual_sites = result['site_name'].tolist()
                self.assertEqual(set(actual_sites), set(expected_sites))

    def test_extract_sites_missing_columns(self):
        """Test extraction when some metadata columns are missing."""
        config = {
            'file': 'test_chemical_data.csv',
            'site_column': 'SiteName',
            'lat_column': 'Latitude',
            'lon_column': 'Longitude',
            'county_column': 'County',
            'basin_column': 'RiverBasin',
            'ecoregion_column': None,  # This column doesn't exist in chemical data
            'description': 'Test chemical data'
        }
        
        with patch('data_processing.consolidate_sites.pd.read_csv') as mock_read_csv:
            with patch('data_processing.consolidate_sites.os.path.exists', return_value=True):
                mock_read_csv.return_value = self.sample_chemical_data
                
                result = extract_sites_from_csv(config)
                
                # Should still return data but with None for missing ecoregion
                self.assertEqual(len(result), 3)
                self.assertTrue(result['ecoregion'].isnull().all())

    def test_extract_sites_file_not_found(self):
        """Test behavior when CSV file doesn't exist."""
        config = {
            'file': 'nonexistent.csv',
            'site_column': 'SiteName',
            'lat_column': 'Latitude',
            'lon_column': 'Longitude',
            'county_column': 'County',
            'basin_column': 'RiverBasin',
            'ecoregion_column': 'Mod_Ecoregion',
            'description': 'Nonexistent file'
        }
        
        with patch('data_processing.consolidate_sites.os.path.exists', return_value=False):
            result = extract_sites_from_csv(config)
            
            # Should return empty DataFrame when file doesn't exist
            self.assertTrue(result.empty)

    def test_detect_conflicts_no_conflicts(self):
        """Test conflict detection when there are no conflicts."""
        existing_site = pd.Series({
            'latitude': 35.1234,
            'longitude': -97.1234,
            'county': 'Cleveland',
            'river_basin': 'Canadian',
            'ecoregion': 'Cross Timbers'
        })
        
        new_site = pd.Series({
            'latitude': 35.1234,  # Same coordinates
            'longitude': -97.1234,
            'county': 'Cleveland',  # Same metadata
            'river_basin': 'Canadian',
            'ecoregion': None  # Missing data, not a conflict
        })
        
        conflicts = detect_conflicts('Test Site', existing_site, new_site)
        self.assertEqual(len(conflicts), 0)

    def test_detect_conflicts_coordinate_conflict(self):
        """Test conflict detection for coordinate differences."""
        existing_site = pd.Series({
            'latitude': 35.1234,
            'longitude': -97.1234,
            'county': 'Cleveland',
            'river_basin': 'Canadian',
            'ecoregion': 'Cross Timbers'
        })
        
        new_site = pd.Series({
            'latitude': 35.9999,  # Different latitude
            'longitude': -97.1234,
            'county': 'Cleveland',
            'river_basin': 'Canadian',
            'ecoregion': 'Cross Timbers'
        })
        
        conflicts = detect_conflicts('Test Site', existing_site, new_site)
        self.assertEqual(len(conflicts), 1)
        self.assertIn('latitude', conflicts[0])

    def test_detect_conflicts_metadata_conflict(self):
        """Test conflict detection for metadata differences."""
        existing_site = pd.Series({
            'latitude': 35.1234,
            'longitude': -97.1234,
            'county': 'Cleveland',
            'river_basin': 'Canadian',
            'ecoregion': 'Cross Timbers'
        })
        
        new_site = pd.Series({
            'latitude': 35.1234,
            'longitude': -97.1234,
            'county': 'McClain',  # Different county
            'river_basin': 'Canadian',
            'ecoregion': 'Cross Timbers'
        })
        
        conflicts = detect_conflicts('Test Site', existing_site, new_site)
        self.assertEqual(len(conflicts), 1)
        self.assertIn('county', conflicts[0])

    @patch('data_processing.arcgis_sync.fetch_site_data', return_value=pd.DataFrame())
    @patch('data_processing.consolidate_sites.extract_sites_from_csv')
    def test_consolidate_sites_priority_order(self, mock_extract, mock_fetch_sites):
        """Test that sites are processed in correct priority order."""
        # Mock the extract function to return different data for different configs
        def side_effect(config):
            if 'site_data' in config['file']:
                return pd.DataFrame({
                    'site_name': ['Blue Creek at Highway 9'],
                    'latitude': [35.1234],
                    'longitude': [-97.1234],
                    'county': ['Cleveland'],
                    'river_basin': ['Canadian'],
                    'ecoregion': ['Cross Timbers'],
                    'source_file': ['site_data.csv'],
                    'source_description': ['Master site data']
                })
            elif 'chemical_data' in config['file']:
                return pd.DataFrame({
                    'site_name': ['Blue Creek at Highway 9', 'New Chemical Site'],
                    'latitude': [35.1234, 34.5555],
                    'longitude': [-97.1234, -96.5555],
                    'county': ['Cleveland', 'Pontotoc'],
                    'river_basin': ['Canadian', 'Red'],
                    'ecoregion': [None, None],
                    'source_file': ['chemical_data.csv', 'chemical_data.csv'],
                    'source_description': ['Original chemical data', 'Original chemical data']
                })
            else:
                return pd.DataFrame()

        mock_extract.side_effect = side_effect

        consolidated_sites, conflicts_df = consolidate_sites()
        
        # Should have 2 sites total (1 from site_data, 1 new from chemical_data)
        self.assertEqual(len(consolidated_sites), 2)
        
        # Blue Creek should have data from site_data (higher priority)
        blue_creek = consolidated_sites[consolidated_sites['site_name'] == 'Blue Creek at Highway 9']
        self.assertEqual(len(blue_creek), 1)
        self.assertEqual(blue_creek.iloc[0]['ecoregion'], 'Cross Timbers')  # From site_data
        
        # Should have no conflicts since no conflicting data
        self.assertTrue(conflicts_df.empty)

    @patch('data_processing.arcgis_sync.fetch_site_data', return_value=pd.DataFrame())
    @patch('data_processing.consolidate_sites.extract_sites_from_csv')
    def test_consolidate_sites_metadata_filling(self, mock_extract, mock_fetch_sites):
        """Test that missing metadata gets filled from lower priority sources."""
        def side_effect(config):
            if 'site_data' in config['file']:
                return pd.DataFrame({
                    'site_name': ['Blue Creek at Highway 9'],
                    'latitude': [35.1234],
                    'longitude': [-97.1234],
                    'county': [None],  # Missing county in site_data
                    'river_basin': ['Canadian'],
                    'ecoregion': ['Cross Timbers'],
                    'source_file': ['site_data.csv'],
                    'source_description': ['Master site data']
                })
            elif 'chemical_data' in config['file']:
                return pd.DataFrame({
                    'site_name': ['Blue Creek at Highway 9'],
                    'latitude': [35.1234],
                    'longitude': [-97.1234],
                    'county': ['Cleveland'],  # County available in chemical_data
                    'river_basin': ['Canadian'],
                    'ecoregion': [None],
                    'source_file': ['chemical_data.csv'],
                    'source_description': ['Original chemical data']
                })
            else:
                return pd.DataFrame()

        mock_extract.side_effect = side_effect

        consolidated_sites, conflicts_df = consolidate_sites()
        
        # Should have 1 site with county filled from chemical_data
        self.assertEqual(len(consolidated_sites), 1)
        blue_creek = consolidated_sites.iloc[0]
        self.assertEqual(blue_creek['county'], 'Cleveland')  # Filled from chemical_data
        self.assertEqual(blue_creek['ecoregion'], 'Cross Timbers')  # From site_data

    def test_save_consolidated_data(self):
        """Test saving consolidated data to CSV files."""
        # Create sample consolidated data
        consolidated_sites = pd.DataFrame({
            'site_name': ['Site 1', 'Site 2'],
            'latitude': [35.1, 34.5],
            'longitude': [-97.1, -96.5],
            'county': ['County A', 'County B'],
            'river_basin': ['Basin 1', 'Basin 2'],
            'ecoregion': ['Eco 1', 'Eco 2']
        })
        
        conflicts_df = pd.DataFrame({
            'site_name': ['Conflict Site'],
            'conflicts': [['latitude: 35.1 vs 35.2']],
            'existing_source': ['site_data.csv'],
            'new_source': ['chemical_data.csv']
        })
        
        with patch('data_processing.consolidate_sites.os.path.join') as mock_join:
            with patch('data_processing.consolidate_sites.pd.DataFrame.to_csv') as mock_to_csv:
                # Mock the path joins to return fake paths
                mock_join.side_effect = lambda *args: '/fake/path/' + args[-1]
                
                save_consolidated_data(consolidated_sites, conflicts_df)
                
                # Should call to_csv at least twice (sites and conflicts, maybe more)
                self.assertGreaterEqual(mock_to_csv.call_count, 2)
                
                # Verify the files being written to don't reference real paths
                call_args_list = mock_to_csv.call_args_list
                for call_args in call_args_list:
                    # First argument should be the file path
                    file_path = call_args[0][0] if call_args[0] else call_args[1].get('path_or_buf', '')
                    self.assertIn('/fake/path/', str(file_path))

    def test_empty_input_handling(self):
        """Test behavior with empty input data."""
        with patch('data_processing.consolidate_sites.extract_sites_from_csv') as mock_extract, \
             patch('data_processing.arcgis_sync.fetch_site_data', return_value=pd.DataFrame()):
            mock_extract.return_value = pd.DataFrame()  # Always return empty

            consolidated_sites, conflicts_df = consolidate_sites()

            # Should handle empty input gracefully
            self.assertTrue(consolidated_sites.empty)
            self.assertTrue(conflicts_df.empty)

    def test_site_name_cleaning(self):
        """Test that site names are properly cleaned during extraction."""
        # Test data with whitespace issues
        messy_data = pd.DataFrame({
            'SiteName': ['  Blue Creek at Highway 9  ', 'Tenmile Creek at Davis\n', 'Red  River   at Bridge'],
            'Latitude': [35.1234, 34.5678, 33.9876],
            'Longitude': [-97.1234, -96.5678, -95.9876]
        })
        
        config = {
            'file': 'test.csv',
            'site_column': 'SiteName',
            'lat_column': 'Latitude',
            'lon_column': 'Longitude',
            'county_column': None,
            'basin_column': None,
            'ecoregion_column': None,
            'description': 'Test data'
        }
        
        with patch('data_processing.consolidate_sites.pd.read_csv') as mock_read_csv:
            with patch('data_processing.consolidate_sites.os.path.exists', return_value=True):
                mock_read_csv.return_value = messy_data
                
                result = extract_sites_from_csv(config)
                
                # Site names should be cleaned of extra whitespace
                expected_clean_names = [
                    'Blue Creek at Highway 9',
                    'Tenmile Creek at Davis',
                    'Red River at Bridge'
                ]
                actual_names = result['site_name'].tolist()
                self.assertEqual(actual_names, expected_clean_names)

    # =============================================================================
    # DUPLICATE MERGING TESTS (from merge_sites.py)
    # =============================================================================

    @patch('data_processing.merge_sites.get_connection')
    def test_find_duplicate_coordinate_groups_with_duplicates(self, mock_get_connection):
        """Test finding coordinate duplicates when they exist."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn
        
        with patch('data_processing.merge_sites.pd.read_sql_query') as mock_read_sql:
            mock_read_sql.return_value = self.sample_sites_with_duplicates
            
            result = find_duplicate_coordinate_groups()
            
            # Should return only the duplicate sites (4 out of 5)
            self.assertEqual(len(result), 4)

    @patch('data_processing.merge_sites.get_connection')
    def test_find_duplicate_coordinate_groups_detects_nearby_sites(self, mock_get_connection):
        """Test Haversine clustering catches near-duplicates."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        # These points are ~0.22m apart but round into different 0.001-degree bins.
        near_boundary_sites = pd.DataFrame({
            'site_id': [1, 2],
            'site_name': ['Site_A (Upstream)', 'Site_A (Downstream)'],
            'latitude': [35.123501, 35.123499],
            'longitude': [-97.5, -97.5],
            'county': [None, None],
            'river_basin': [None, None],
            'ecoregion': [None, None],
        })

        with patch('data_processing.merge_sites.pd.read_sql_query') as mock_read_sql:
            mock_read_sql.return_value = near_boundary_sites

            result_default = find_duplicate_coordinate_groups()
            self.assertEqual(len(result_default), 2)
            self.assertIn('group_id', result_default.columns)
            self.assertEqual(len(result_default['group_id'].unique()), 1)

    @patch('data_processing.merge_sites.get_connection')
    def test_find_duplicate_coordinate_groups_respects_threshold(self, mock_get_connection):
        """Test Haversine clustering does not create clusters when points exceed threshold."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        far_sites = pd.DataFrame({
            'site_id': [1, 2],
            'site_name': ['Site_Far_A', 'Site_Far_B'],
            'latitude': [35.123501, 35.124000],
            'longitude': [-97.5, -97.5],
            'county': [None, None],
            'river_basin': [None, None],
            'ecoregion': [None, None],
        })

        with patch('data_processing.merge_sites.pd.read_sql_query') as mock_read_sql:
            mock_read_sql.return_value = far_sites
            result = find_duplicate_coordinate_groups(distance_threshold_m=10.0)
            self.assertTrue(result.empty)
            self.assertIn('group_id', result.columns)

    @patch('data_processing.merge_sites.get_connection')
    def test_find_duplicate_coordinate_groups_transitive_closure(self, mock_get_connection):
        """Test clusters are transitive (A~B and B~C implies A,B,C grouped)."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        chain_sites = pd.DataFrame({
            'site_id': [1, 2, 3],
            'site_name': ['Site_Chain_A', 'Site_Chain_B', 'Site_Chain_C'],
            'latitude': [35.123000, 35.123100, 35.123200],
            'longitude': [-97.5, -97.5, -97.5],
            'county': [None, None, None],
            'river_basin': [None, None, None],
            'ecoregion': [None, None, None],
        })

        with patch('data_processing.merge_sites.pd.read_sql_query') as mock_read_sql:
            mock_read_sql.return_value = chain_sites

            result_default = find_duplicate_coordinate_groups(distance_threshold_m=15.0)
            self.assertEqual(len(result_default), 3)
            self.assertEqual(len(result_default['group_id'].unique()), 1)

    @patch('data_processing.merge_sites.get_connection')
    def test_find_duplicate_coordinate_groups_negative_longitude_bins(self, mock_get_connection):
        """Test detection works with negative longitude floor-bin boundaries."""
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        negative_lon_sites = pd.DataFrame({
            'site_id': [1, 2],
            'site_name': ['NegLon_A', 'NegLon_B'],
            'latitude': [35.123501, 35.123499],
            'longitude': [-97.500001, -97.499999],
            'county': [None, None],
            'river_basin': [None, None],
            'ecoregion': [None, None],
        })

        with patch('data_processing.merge_sites.pd.read_sql_query') as mock_read_sql:
            mock_read_sql.return_value = negative_lon_sites

            result_default = find_duplicate_coordinate_groups()
            self.assertEqual(len(result_default), 2)
            self.assertEqual(len(result_default['group_id'].unique()), 1)

    def test_determine_preferred_site_updated_chemical_priority(self):
        """Test site selection prioritizes updated_chemical_data sites."""
        group = self.sample_sites_with_duplicates[
            self.sample_sites_with_duplicates['site_id'].isin([1, 2])
        ].copy()
        
        preferred_site, sites_to_merge, reason = determine_preferred_site(
            group, self.updated_chemical_sites, self.chemical_data_sites
        )
        
        # Should prefer the site in updated_chemical_data
        self.assertEqual(preferred_site['site_name'], 'Blue Creek Site A')
        self.assertIn("updated_chemical", reason.lower())
        self.assertEqual(len(sites_to_merge), 1)

    def test_determine_preferred_site_chemical_data_priority(self):
        """Test site selection falls back to chemical_data sites."""
        group = self.sample_sites_with_duplicates[
            self.sample_sites_with_duplicates['site_id'].isin([3, 4])
        ].copy()
        
        preferred_site, sites_to_merge, reason = determine_preferred_site(
            group, self.updated_chemical_sites, self.chemical_data_sites
        )
        
        # Should prefer the site in chemical_data
        self.assertEqual(preferred_site['site_name'], 'Red River Main')
        self.assertIn("chemical_data", reason.lower())

    @patch('data_processing.merge_sites.get_connection')
    def test_transfer_site_data_basic(self, mock_get_connection):
        """Test transferring data between sites."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock the verification and count queries
        mock_cursor.fetchone.side_effect = [
            ('Source Site',),  # Source site exists verification
            ('Dest Site',),    # Destination site exists verification
            (5,),  # chemical_collection_events count
            (3,),  # fish_collection_events count
            (2,),  # macro_collection_events count
            (1,)   # habitat_assessments count
        ]
        
        # Mock rowcount for update operations to match the counts
        # The function returns cursor.rowcount, not the original counts
        mock_cursor.rowcount = MagicMock()
        mock_cursor.rowcount.__iter__ = lambda x: iter([5, 3, 2, 1])  # Return matching rowcounts
        
        # Create a side effect that returns the expected rowcount for each update
        rowcount_values = [5, 3, 2, 1]
        call_count = 0
        def rowcount_side_effect():
            nonlocal call_count
            if call_count < len(rowcount_values):
                value = rowcount_values[call_count]
                call_count += 1
                return value
            return 0
        
        # Use a property mock to simulate rowcount behavior
        type(mock_cursor).rowcount = PropertyMock(side_effect=rowcount_side_effect)
        
        result = transfer_site_data(mock_cursor, from_site_id=2, to_site_id=1)
        
        # Should return counts based on rowcount (actual rows affected)
        expected_total = 5 + 3 + 2 + 1  # Sum of rowcounts
        total_transferred = sum(result.values())
        self.assertEqual(total_transferred, expected_total)

    @patch('data_processing.merge_sites.load_csv_files')
    @patch('data_processing.merge_sites.find_duplicate_coordinate_groups')
    @patch('data_processing.merge_sites.get_connection')
    def test_analyze_coordinate_duplicates_with_data(self, mock_get_connection, mock_find_dupes, mock_load_csv):
        """Test analyzing coordinate duplicates when duplicates exist."""
        # Mock CSV loading
        mock_load_csv.return_value = (
            pd.DataFrame({'SiteName': ['Site A', 'Site B']}),
            pd.DataFrame({'Site Name': ['Blue Creek Site A']}),
            pd.DataFrame({'SiteName': ['Red River Main']})
        )
        
        # Mock finding duplicates — include group_id for default boundary-safe mode
        dupes = self.sample_sites_with_duplicates[
            self.sample_sites_with_duplicates['site_id'].isin([1, 2])
        ].copy()
        dupes['group_id'] = 0
        mock_find_dupes.return_value = dupes

        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        # Default (boundary-safe) should work with group_id data
        result = analyze_coordinate_duplicates()

        # Should return analysis results
        self.assertIsNotNone(result)
        self.assertIn('total_duplicate_sites', result)
        self.assertEqual(result['total_duplicate_sites'], 2)

    @patch('data_processing.merge_sites.close_connection')
    @patch('data_processing.merge_sites.get_connection')
    @patch('data_processing.merge_sites.find_duplicate_coordinate_groups')
    @patch('data_processing.merge_sites.load_csv_files')
    def test_analyze_coordinate_duplicates_with_group_labels(
        self,
        mock_load_csv,
        mock_find_dupes,
        mock_get_connection,
        mock_close_connection,
    ):
        """Test analyzing clusters reports groups and uses group_id labels."""
        mock_load_csv.return_value = (
            pd.DataFrame({'SiteName': ['Site A', 'Site B']}),
            pd.DataFrame({'Site Name': ['Site_A (Upstream)']}),
            pd.DataFrame({'SiteName': []}),
        )

        df = pd.DataFrame({
            'site_id': [1, 2],
            'site_name': ['Site_A (Upstream)', 'Site_A (Downstream)'],
            'latitude': [35.123501, 35.123499],
            'longitude': [-97.5, -97.5],
            'county': [None, None],
            'river_basin': [None, None],
            'ecoregion': [None, None],
            'group_id': [0, 0],
        })
        mock_find_dupes.return_value = df

        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        result = analyze_coordinate_duplicates()

        self.assertIsNotNone(result)
        self.assertEqual(result['total_duplicate_sites'], 2)
        self.assertEqual(result['duplicate_groups'], 1)
        self.assertTrue(any('group_id=' in ex['coordinates'] for ex in result['examples']))

    @patch('data_processing.merge_sites.update_csv_files_with_mapping')
    @patch('data_processing.merge_sites.update_site_metadata')
    @patch('data_processing.merge_sites.transfer_site_data')
    @patch('data_processing.merge_sites.find_duplicate_coordinate_groups')
    @patch('data_processing.merge_sites.load_csv_files')
    @patch('data_processing.merge_sites.get_connection')
    def test_merge_duplicate_sites_merges_one_group(
        self,
        mock_get_connection,
        mock_load_csv,
        mock_find_dupes,
        mock_transfer,
        mock_update_site_metadata,
        mock_update_csv,
    ):
        """Test merge_duplicate_sites groups by group_id and deletes extras."""
        mock_load_csv.return_value = (
            pd.DataFrame({'SiteName': ['Site A']}),
            pd.DataFrame({'Site Name': ['Site_Keep']}),
            pd.DataFrame({'SiteName': []}),
        )

        df = pd.DataFrame({
            'site_id': [1, 2],
            'site_name': ['Site_Keep', 'Site_Delete'],
            'latitude': [35.123501, 35.123499],
            'longitude': [-97.5, -97.5],
            'county': [None, None],
            'river_basin': [None, None],
            'ecoregion': [None, None],
            'group_id': [0, 0],
        })
        mock_find_dupes.return_value = df

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Preferred site existence check
        mock_cursor.fetchone.return_value = ('Site_Keep',)

        mock_transfer.return_value = {
            'chemical_collection_events': 1,
            'fish_collection_events': 0,
            'macro_collection_events': 0,
            'habitat_assessments': 0,
        }
        mock_update_site_metadata.return_value = True

        result = merge_duplicate_sites()

        self.assertEqual(result['groups_processed'], 1)
        self.assertEqual(result['sites_deleted'], 1)
        self.assertEqual(result['records_transferred'], 1)
        self.assertTrue(mock_transfer.called)

    # =============================================================================
    # SITE PROCESSING TESTS (from site_processing.py)
    # =============================================================================

    @patch('data_processing.site_processing.pd.read_csv')
    @patch('data_processing.site_processing.os.path.exists')
    @patch('data_processing.site_processing.pd.DataFrame.to_csv')  # Mock the to_csv call
    @patch('data_processing.site_processing.os.path.join')  # Mock path joining
    def test_load_site_data_basic(self, mock_join, mock_to_csv, mock_exists, mock_read_csv):
        """Test basic site data loading functionality."""
        # Mock file exists
        mock_exists.return_value = True
        
        # Mock path join to return fake path
        mock_join.return_value = '/fake/path/sites_for_db.csv'
        
        # Mock CSV data
        mock_read_csv.return_value = pd.DataFrame({
            'site_name': ['Site 1', 'Site 2'],
            'latitude': [35.1, 34.5],
            'longitude': [-97.1, -96.5],
            'county': ['County A', 'County B'],
            'river_basin': ['Basin 1', 'Basin 2'],
            'ecoregion': ['Eco 1', 'Eco 2']
        })
        
        result = load_site_data()
        
        # Should return the expected data
        self.assertFalse(result.empty)
        self.assertEqual(len(result), 2)
        self.assertIn('site_name', result.columns)
        
        # Should call to_csv to save the processed data (but mocked)
        mock_to_csv.assert_called_once()
        
        # Verify it's trying to write to our mocked path
        mock_to_csv.assert_called_with('/fake/path/sites_for_db.csv', index=False)

    @patch('data_processing.site_processing.os.path.exists')
    def test_load_site_data_file_not_found(self, mock_exists):
        """Test loading site data when master_sites.csv doesn't exist."""
        mock_exists.return_value = False
        
        result = load_site_data()
        
        self.assertTrue(result.empty)

    @patch('data_processing.site_processing.close_connection')
    @patch('data_processing.site_processing.get_connection')
    def test_insert_sites_into_db_basic(self, mock_get_conn, mock_close):
        """Test inserting sites into database."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock that sites exist (so they get updated rather than inserted)
        mock_cursor.fetchone.side_effect = [
            (1,),  # First site exists with ID 1
            (2,)   # Second site exists with ID 2
        ]
        
        result = insert_sites_into_db(self.sample_master_sites)
        
        self.assertEqual(result, 2)  # Should process 2 sites
        # Should call execute (for updates) instead of executemany
        self.assertGreaterEqual(mock_cursor.execute.call_count, 2)  # At least site checks + updates
        mock_conn.commit.assert_called_once()

    def test_insert_sites_into_db_empty_data(self):
        """Test inserting empty site data."""
        empty_df = pd.DataFrame()
        
        result = insert_sites_into_db(empty_df)
        
        self.assertEqual(result, 0)

    @patch('data_processing.site_processing.insert_sites_into_db')
    @patch('data_processing.site_processing.load_site_data')
    def test_process_site_data_success(self, mock_load, mock_insert):
        """Test successful site data processing."""
        # Mock load_site_data to return sample data without file operations
        mock_load.return_value = pd.DataFrame({
            'site_name': ['Test Site'],
            'latitude': [35.1],
            'longitude': [-97.1]
        })
        
        # Mock database insertion
        mock_insert.return_value = 1
        
        result = process_site_data()
        
        self.assertTrue(result)
        mock_load.assert_called_once()
        mock_insert.assert_called_once()

    @patch('data_processing.site_processing.load_site_data')
    def test_process_site_data_load_failure(self, mock_load):
        """Test site data processing when loading fails."""
        mock_load.return_value = pd.DataFrame()  # Empty result indicates failure
        
        result = process_site_data()
        
        self.assertFalse(result)

    @patch('data_processing.site_processing.close_connection')
    @patch('data_processing.site_processing.get_connection')
    def test_cleanup_unused_sites(self, mock_get_conn, mock_close):
        """Test cleanup of sites with no monitoring data."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock database responses
        mock_cursor.fetchall.side_effect = [
            [(1,), (2,)],  # Sites with data
            [(1,), (2,), (3,)]  # All sites
        ]
        
        result = cleanup_unused_sites()
        
        self.assertTrue(result)
        # Should delete unused sites
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()

    @patch('data_processing.site_processing.close_connection')
    @patch('data_processing.site_processing.get_connection')
    def test_classify_active_sites(self, mock_get_conn, mock_close):
        """Test classifying sites as active or historic."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock database responses
        mock_cursor.fetchone.return_value = ('2023-12-01',)  # Most recent date
        mock_cursor.fetchall.return_value = [
            (1, 'Blue Creek at Highway 9', '2023-11-15'),  # Recent - active
            (2, 'Old Creek', '2020-05-20')  # Old - historic
        ]
        
        result = classify_active_sites()
        
        self.assertTrue(result)
        self.assertGreaterEqual(mock_cursor.execute.call_count, 3)  # At least 1 max query + 2 updates
        mock_conn.commit.assert_called_once()

    @patch('data_processing.arcgis_sync.fetch_site_data', return_value=pd.DataFrame())
    @patch('data_processing.consolidate_sites.extract_sites_from_csv')
    @patch('data_processing.consolidate_sites.save_consolidated_data')  # Mock the save function
    def test_consolidate_sites_no_file_writes(self, mock_save, mock_extract, mock_fetch_sites):
        """Test that consolidate_sites doesn't write to real files during testing."""
        # Mock extract to return minimal test data
        mock_extract.return_value = pd.DataFrame({
            'site_name': ['Test Site'],
            'latitude': [35.1],
            'longitude': [-97.1],
            'county': [None],
            'river_basin': [None],
            'ecoregion': [None],
            'source_file': ['test.csv'],
            'source_description': ['Test data']
        })
        
        consolidated_sites, conflicts_df = consolidate_sites()
        
        # Should have extracted the test site
        self.assertEqual(len(consolidated_sites), 1)
        self.assertEqual(consolidated_sites.iloc[0]['site_name'], 'Test Site')
        
        # Save function should NOT be called automatically by consolidate_sites
        # The save is handled separately in main() 
        mock_save.assert_not_called()


if __name__ == '__main__':
    # Set up test discovery and run tests
    unittest.main(verbosity=2)