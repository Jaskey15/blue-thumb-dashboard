"""
Test suite for chemical data processing functionality.
Tests the logic in data_processing.chemical_processing module.
"""

import os
import sqlite3
import sys
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from data_processing.chemical_processing import process_chemical_data_from_csv

# Note: chemical_duplicates functionality was removed - tests for that functionality are no longer needed
from data_processing.chemical_utils import (
    apply_bdl_conversions,
    calculate_soluble_nitrogen,
    convert_bdl_value,
    remove_empty_chemical_rows,
    validate_chemical_data,
)
from data_processing.updated_chemical_processing import (
    format_to_database_schema,
    get_conditional_nutrient_value,
    get_greater_value,
    get_ph_worst_case,
    parse_sampling_dates,
    process_conditional_nutrient,
    process_simple_nutrients,
    process_updated_chemical_data,
)
from data_processing.arcgis_sync import translate_to_pipeline_schema
from data_processing.chemical_utils import insert_collection_event
from utils import setup_logging

# Set up logging for tests
logger = setup_logging("test_chemical_processing", category="testing")


class TestChemicalProcessing(unittest.TestCase):
    """Comprehensive test suite for all chemical processing functionality."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Sample chemical data for testing (matching your real CSV structure)
        self.sample_chemical_data = pd.DataFrame({
            'SiteName': ['Blue Creek at Highway 9', 'Tenmile Creek at Davis', 'Red River at Bridge'],
            'Date': ['2023-05-15', '2023-06-20', '2023-07-10'],
            'DO.Saturation': [95.5, 110.2, 88.0],
            'pH.Final.1': [7.2, 8.1, 6.8],
            'Nitrate.Final.1': [0.5, 0.0, 1.2],  # Zero for BDL testing
            'Nitrite.Final.1': [0.0, 0.05, 0.1],  # Zero for BDL testing
            'Ammonia.Final.1': [0.1, 0.0, 0.3],  # Zero for BDL testing
            'OP.Final.1': [0.02, 0.08, 0.15],
            'Chloride.Final.1': [25.0, 45.0, 280.0]  # Last value exceeds threshold
        })
        
        # Sample reference values for testing
        self.sample_reference_values = {
            'do_percent': {
                'normal min': 80, 
                'normal max': 130, 
                'caution min': 50,
                'caution max': 150
            },
            'pH': {
                'normal min': 6.5, 
                'normal max': 9.0
            },
            'soluble_nitrogen': {
                'normal': 0.8, 
                'caution': 1.5
            },
            'Phosphorus': {
                'normal': 0.05, 
                'caution': 0.1
            },
            'Chloride': {
                'poor': 250
            }
        }
        
        # Sample updated chemical data for testing
        self.sample_updated_data = pd.DataFrame({
            'Site Name': ['Test Site 1', 'Test Site 2'],
            'Sampling Date': ['5/15/2023, 10:30 AM', '6/20/2023, 2:15 PM'],
            '% Oxygen Saturation': [95.5, 110.2],
            'pH #1': [7.2, 8.1],
            'pH #2': [7.3, 8.0],
            'Nitrate #1': [0.5, 1.2],
            'Nitrate #2': [0.6, 1.1],
            'Nitrite #1': [0.05, 0.1],
            'Nitrite #2': [0.04, 0.12],
            'Ammonia Nitrogen Range Selection': ['Low', 'Mid'],
            'Ammonia Nitrogen Low Reading #1': [0.1, 0.3],
            'Ammonia Nitrogen Low Reading #2': [0.12, 0.28],
            'Ammonia_nitrogen_midrange1_Final': [0.2, 0.4],
            'Ammonia_nitrogen_midrange2_Final': [0.22, 0.38],
            'Orthophosphate Range Selection': ['Low', 'High'],
            'Orthophosphate_Low1_Final': [0.02, 0.15],
            'Orthophosphate_Low2_Final': [0.03, 0.14],
            'Orthophosphate_High1_Final': [0.1, 0.5],
            'Orthophosphate_High2_Final': [0.12, 0.48],
            'Chloride Range Selection': ['Low', 'High'],
            'Chloride_Low1_Final': [25.0, 45.0],
            'Chloride_Low2_Final': [26.0, 44.0],
            'Chloride_High1_Final': [250.0, 280.0],
            'Chloride_High2_Final': [255.0, 275.0]
        })

    # =============================================================================
    # UTILITY FUNCTION TESTS
    # =============================================================================

    def test_convert_bdl_value_basic(self):
        """Test basic BDL value conversion."""
        # Test zero conversion
        self.assertEqual(convert_bdl_value(0, 0.3), 0.3)
        
        # Test non-zero value unchanged
        self.assertEqual(convert_bdl_value(1.5, 0.3), 1.5)
        
        # Test NaN handling
        self.assertTrue(np.isnan(convert_bdl_value(np.nan, 0.3)))
        
        # Test string zero conversion
        self.assertEqual(convert_bdl_value("0", 0.3), 0.3)
        
        # Test invalid string handling
        self.assertTrue(np.isnan(convert_bdl_value("invalid", 0.3)))

    def test_apply_bdl_conversions(self):
        """Test applying BDL conversions to DataFrame columns."""
        # Create test data with zeros
        test_df = pd.DataFrame({
            'Nitrate': [0.5, 0.0, 1.2],
            'Nitrite': [0.0, 0.05, 0.1],
            'Ammonia': [0.1, 0.0, 0.3],
            'Other_Param': [1.0, 2.0, 3.0]  # Should not be affected
        })
        
        result_df = apply_bdl_conversions(test_df)
        
        # Check that zeros were replaced with BDL values
        self.assertEqual(result_df.loc[1, 'Nitrate'], 0.3)  # BDL_VALUES['Nitrate']
        self.assertEqual(result_df.loc[0, 'Nitrite'], 0.03)  # BDL_VALUES['Nitrite']
        self.assertEqual(result_df.loc[1, 'Ammonia'], 0.03)  # BDL_VALUES['Ammonia']
        
        # Check that non-zero values were unchanged
        self.assertEqual(result_df.loc[0, 'Nitrate'], 0.5)
        self.assertEqual(result_df.loc[1, 'Nitrite'], 0.05)
        
        # Check that non-BDL columns were unchanged
        self.assertEqual(result_df.loc[0, 'Other_Param'], 1.0)

    def test_calculate_soluble_nitrogen(self):
        """Test calculation of total soluble nitrogen."""
        # Create test data with nitrogen components
        test_df = pd.DataFrame({
            'Nitrate': [0.5, 1.0, 0.0],
            'Nitrite': [0.1, 0.05, 0.0],
            'Ammonia': [0.2, 0.15, 0.0]
        })
        
        result_df = calculate_soluble_nitrogen(test_df)
        
        # Check that soluble_nitrogen was calculated correctly
        self.assertIn('soluble_nitrogen', result_df.columns)
        
        # First row: 0.5 + 0.1 + 0.2 = 0.8
        self.assertAlmostEqual(result_df.loc[0, 'soluble_nitrogen'], 0.8, places=3)
        
        # Second row: 1.0 + 0.05 + 0.15 = 1.2
        self.assertAlmostEqual(result_df.loc[1, 'soluble_nitrogen'], 1.2, places=3)
        
        # Third row: All zeros should use BDL values: 0.3 + 0.03 + 0.03 = 0.36
        self.assertAlmostEqual(result_df.loc[2, 'soluble_nitrogen'], 0.36, places=3)

    def test_validate_chemical_data_basic(self):
        """Test basic chemical data validation."""
        # Create test data with some invalid values
        test_df = pd.DataFrame({
            'do_percent': [95.5, 250.0, 88.0],  # Second value too high
            'pH': [7.2, 15.0, 6.8],  # Second value invalid
            'Chloride': [25.0, -5.0, 45.0]  # Second value negative
        })
        
        result_df = validate_chemical_data(test_df, remove_invalid=True)
        
        # Check that invalid values were removed (set to NaN)
        self.assertTrue(pd.isna(result_df.loc[1, 'pH']))  # pH 15.0 should be removed
        self.assertTrue(pd.isna(result_df.loc[1, 'Chloride']))  # Negative chloride should be removed
        
        # Check that valid values remain
        self.assertEqual(result_df.loc[0, 'do_percent'], 95.5)
        self.assertEqual(result_df.loc[0, 'pH'], 7.2)

    def test_remove_empty_chemical_rows(self):
        """Test removal of rows with no chemical data."""
        # Create test data with some empty rows
        test_df = pd.DataFrame({
            'Site_Name': ['Site1', 'Site2', 'Site3', 'Site4'],
            'Date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
            'do_percent': [95.5, np.nan, 88.0, np.nan],
            'pH': [7.2, np.nan, np.nan, np.nan],
            'Nitrate': [0.5, np.nan, np.nan, np.nan],
            'Other_Col': ['A', 'B', 'C', 'D']  # Non-chemical column
        })
        
        result_df = remove_empty_chemical_rows(test_df)
        
        # Should keep rows with at least one chemical parameter
        self.assertEqual(len(result_df), 2)  # Only Site1 and Site3 have chemical data
        self.assertIn('Site1', result_df['Site_Name'].values)
        self.assertIn('Site3', result_df['Site_Name'].values)
        
        # Non-chemical column should be preserved
        self.assertIn('Other_Col', result_df.columns)

    # =============================================================================
    # UPDATED PROCESSING FUNCTION TESTS
    # =============================================================================

    def test_parse_sampling_dates(self):
        """Test parsing of sampling dates from datetime strings."""
        # Create test data with datetime strings
        test_df = pd.DataFrame({
            'Sampling Date': [
                '5/15/2023, 10:30 AM',
                '6/20/2023, 2:15 PM',
                '7/10/2023, 9:45 AM'
            ]
        })
        
        result_df = parse_sampling_dates(test_df)
        
        # Check that Date column was created
        self.assertIn('Date', result_df.columns)
        
        # Check that dates were parsed correctly
        self.assertEqual(result_df['Date'].dt.year.iloc[0], 2023)
        self.assertEqual(result_df['Date'].dt.month.iloc[0], 5)
        self.assertEqual(result_df['Date'].dt.day.iloc[0], 15)
        
        # Check that Year and Month columns were added
        self.assertIn('Year', result_df.columns)
        self.assertIn('Month', result_df.columns)
        
        # Check that intermediate column was removed
        self.assertNotIn('parsed_datetime', result_df.columns)

    def test_translate_to_pipeline_schema_normalizes_site_and_sets_sample_id(self):
        """ArcGIS translation should normalize whitespace and carry objectid as sample_id."""
        record = {
            'objectid': 123456,
            'SiteName': 'Wolf Creek:  McMahon Soccer Park',
            'day': 1737374400000,  # 2025-01-20 12:00:00 UTC
            'oxygen_sat': 95.5,
            'pH1': 7.2,
            'pH2': 7.5,
            'nitratetest1': 0.5,
            'nitratetest2': 0.6,
            'nitritetest1': 0.05,
            'nitritetest2': 0.04,
            'Ammonia_Range': 'Low',
            'ammonia_Nitrogen2': 0.1,
            'ammonia_Nitrogen3': 0.12,
            'Ammonia_nitrogen_midrange1_Final': None,
            'Ammonia_nitrogen_midrange2_Final': None,
            'Ortho_Range': 'Low',
            'Orthophosphate_Low1_Final': 0.02,
            'Orthophosphate_Low2_Final': 0.03,
            'Orthophosphate_Mid1_Final': None,
            'Orthophosphate_Mid2_Final': None,
            'Orthophosphate_High1_Final': None,
            'Orthophosphate_High2_Final': None,
            'Chloride_Range': 'Low',
            'Chloride_Low1_Final': 25.0,
            'Chloride_Low2_Final': 26.0,
            'Chloride_High1_Final': None,
            'Chloride_High2_Final': None,
            'QAQC_Complete': 'X',
        }

        df = translate_to_pipeline_schema([record])
        self.assertEqual(len(df), 1)
        self.assertIn('sample_id', df.columns)
        self.assertEqual(df.loc[0, 'sample_id'], 123456)
        self.assertEqual(df.loc[0, 'Site Name'], 'Wolf Creek: McMahon Soccer Park')
        self.assertIn('Sampling Date', df.columns)

        parsed = parse_sampling_dates(df.copy())
        self.assertIn('Date', parsed.columns)
        self.assertIn('Year', parsed.columns)
        self.assertIn('Month', parsed.columns)
        self.assertIn('sample_id', parsed.columns)

    def test_format_to_database_schema_preserves_sample_id(self):
        """When sample_id is present, it should be retained in formatted output."""
        test_df = pd.DataFrame({
            'Site Name': ['Test Site'],
            'Date': [pd.Timestamp('2025-01-20')],
            'Year': [2025],
            'Month': [1],
            '% Oxygen Saturation': [95.5],
            'pH #1': [7.2],
            'pH #2': [7.5],
            'Nitrate': [0.6],
            'Nitrite': [0.05],
            'Ammonia': [0.12],
            'Orthophosphate': [0.03],
            'Chloride': [26.0],
            'soluble_nitrogen': [0.77],
            'sample_id': [123456],
        })

        formatted = format_to_database_schema(test_df)
        self.assertFalse(formatted.empty)
        self.assertIn('sample_id', formatted.columns)
        self.assertEqual(formatted.loc[0, 'sample_id'], 123456)

    def test_insert_collection_event_idempotent_with_sample_id(self):
        """insert_collection_event should reuse an event when sample_id matches."""
        conn = sqlite3.connect(':memory:')
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE chemical_collection_events (
                event_id INTEGER PRIMARY KEY,
                site_id INTEGER NOT NULL,
                sample_id INTEGER,
                collection_date TEXT NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL
            );
            """
        )

        cursor.execute(
            """
            CREATE UNIQUE INDEX idx_chemical_collection_events_sample_id
            ON chemical_collection_events(sample_id)
            WHERE sample_id IS NOT NULL
            """
        )

        event_id_1 = insert_collection_event(
            cursor,
            site_id=1,
            date_str='2025-01-20',
            year=2025,
            month=1,
            site_name='Test Site',
            sample_id=999,
        )
        event_id_2 = insert_collection_event(
            cursor,
            site_id=1,
            date_str='2025-01-20',
            year=2025,
            month=1,
            site_name='Test Site',
            sample_id=999,
        )

        self.assertEqual(event_id_1, event_id_2)
        cursor.execute("SELECT COUNT(*) FROM chemical_collection_events")
        self.assertEqual(cursor.fetchone()[0], 1)

        event_id_3 = insert_collection_event(
            cursor,
            site_id=1,
            date_str='2025-01-20',
            year=2025,
            month=1,
            site_name='Test Site',
            sample_id=1000,
        )
        self.assertNotEqual(event_id_1, event_id_3)
        cursor.execute("SELECT COUNT(*) FROM chemical_collection_events")
        self.assertEqual(cursor.fetchone()[0], 2)

    def test_get_ph_worst_case(self):
        """Test logic for selecting pH value furthest from neutral."""
        # Test case where pH #2 is further from 7 (more acidic)
        row1 = pd.Series({'pH #1': 7.5, 'pH #2': 6.0})
        self.assertEqual(get_ph_worst_case(row1), 6.0)

        # Test case where pH #1 is further from 7 (more basic)
        row2 = pd.Series({'pH #1': 8.5, 'pH #2': 7.2})
        self.assertEqual(get_ph_worst_case(row2), 8.5)

        # Test case with equidistant values (prefers pH #1 as tie-breaker)
        row3 = pd.Series({'pH #1': 6.0, 'pH #2': 8.0})
        self.assertEqual(get_ph_worst_case(row3), 6.0)

        # Test case with one null value
        row4 = pd.Series({'pH #1': 7.8, 'pH #2': np.nan})
        self.assertEqual(get_ph_worst_case(row4), 7.8)

        # Test case with both null values
        row5 = pd.Series({'pH #1': None, 'pH #2': None})
        self.assertIsNone(get_ph_worst_case(row5))

        # Test case with invalid string data
        row6 = pd.Series({'pH #1': 'invalid', 'pH #2': 7.9})
        self.assertEqual(get_ph_worst_case(row6), 7.9)

    def test_get_greater_value(self):
        """Test logic for selecting greater of two values."""
        # Create test row
        test_row = pd.Series({
            'col1': 5.0,
            'col2': 7.0,
            'col3': 5.0,  # Equal to col1
            'col4': None,
            'col5': 'invalid'
        })
        
        # Test basic greater value selection
        self.assertEqual(get_greater_value(test_row, 'col1', 'col2'), 7.0)
        
        # Test equal values with tiebreaker
        self.assertEqual(get_greater_value(test_row, 'col1', 'col3', tiebreaker='col1'), 5.0)
        self.assertEqual(get_greater_value(test_row, 'col1', 'col3', tiebreaker='col2'), 5.0)
        
        # Test handling of None values
        self.assertEqual(get_greater_value(test_row, 'col1', 'col4'), 5.0)
        self.assertEqual(get_greater_value(test_row, 'col4', 'col2'), 7.0)
        self.assertIsNone(get_greater_value(test_row, 'col4', 'col4'))
        
        # Test handling of invalid values
        self.assertEqual(get_greater_value(test_row, 'col1', 'col5'), 5.0)
        result = get_greater_value(test_row, 'col5', 'col5')
        self.assertTrue(result is None or pd.isna(result))

    def test_get_conditional_nutrient_value(self):
        """Test range-based nutrient value selection."""
        # Create test row with all ranges
        test_row = pd.Series({
            'range_selection': 'Low',
            'low_col1': 0.1,
            'low_col2': 0.12,
            'mid_col1': 0.2,
            'mid_col2': 0.22,
            'high_col1': 0.3,
            'high_col2': 0.32
        })
        
        # Test low range selection
        self.assertEqual(
            get_conditional_nutrient_value(
                test_row, 'range_selection', 'low_col1', 'low_col2'
            ),
            0.12  # Greater of low range values
        )
        
        # Test mid range selection
        test_row['range_selection'] = 'Mid'
        self.assertEqual(
            get_conditional_nutrient_value(
                test_row, 'range_selection', 'low_col1', 'low_col2',
                mid_col1='mid_col1', mid_col2='mid_col2'
            ),
            0.22  # Greater of mid range values
        )
        
        # Test high range selection
        test_row['range_selection'] = 'High'
        self.assertEqual(
            get_conditional_nutrient_value(
                test_row, 'range_selection', 'low_col1', 'low_col2',
                high_col1='high_col1', high_col2='high_col2'
            ),
            0.32  # Greater of high range values
        )
        
        # Test invalid range selection
        test_row['range_selection'] = 'Invalid'
        self.assertIsNone(
            get_conditional_nutrient_value(
                test_row, 'range_selection', 'low_col1', 'low_col2'
            )
        )

    def test_process_conditional_nutrient(self):
        """Test processing of conditional nutrients."""
        # Create test data with ammonia readings
        test_df = pd.DataFrame({
            'Ammonia Nitrogen Range Selection': ['Low', 'Mid', 'Low', 'Invalid'], 
            'Ammonia Nitrogen Low Reading #1': [0.1, 0.2, 0.3, 0.4],
            'Ammonia Nitrogen Low Reading #2': [0.12, 0.22, 0.32, 0.42],
            'Ammonia_nitrogen_midrange1_Final': [0.2, 0.3, 0.4, 0.5],
            'Ammonia_nitrogen_midrange2_Final': [0.22, 0.32, 0.42, 0.52]
        })
        
        result = process_conditional_nutrient(test_df, 'ammonia')
        
        # Check that values were selected correctly based on range
        self.assertEqual(result.iloc[0], 0.12)  # Low range
        self.assertEqual(result.iloc[1], 0.32)  # Mid range
        self.assertEqual(result.iloc[2], 0.32)  # Low range
        self.assertTrue(pd.isna(result.iloc[3]) or result.iloc[3] is None)  # Invalid range

    def test_process_simple_nutrients(self):
        """Test processing of simple nutrients (Nitrate, Nitrite)."""
        # Create test data
        test_df = pd.DataFrame({
            'Nitrate #1': [0.5, 1.0, 0.0],
            'Nitrate #2': [0.6, 0.9, 0.0],
            'Nitrite #1': [0.05, 0.1, 0.0],
            'Nitrite #2': [0.04, 0.12, 0.0]
        })
        
        result_df = process_simple_nutrients(test_df)
        
        # Check that greater values were selected
        self.assertEqual(result_df['Nitrate'].iloc[0], 0.6)  # Greater of 0.5 and 0.6
        self.assertEqual(result_df['Nitrate'].iloc[1], 1.0)  # Greater of 1.0 and 0.9
        self.assertEqual(result_df['Nitrite'].iloc[0], 0.05)  # Greater of 0.05 and 0.04
        self.assertEqual(result_df['Nitrite'].iloc[1], 0.12)  # Greater of 0.1 and 0.12

    def test_format_to_database_schema(self):
        """Test formatting of data to match database schema."""
        # Create test data
        test_df = pd.DataFrame({
            'Site Name': ['Site1', 'Site2', 'Site3'],
            'Date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
            'Year': [2023, 2023, 2023],
            'Month': [1, 1, 1],
            '% Oxygen Saturation': [95.5, 88.0, 90.0],
            'pH #1': [7.2, 6.8, 8.0],
            'pH #2': [7.5, 6.5, 6.0],
            'Nitrate': [0.5, 1.2, 0.8],
            'Nitrite': [0.05, 0.1, 0.2],
            'Ammonia': [0.1, 0.3, 0.4],
            'Orthophosphate': [0.02, 0.15, 0.1],
            'Chloride': [25.0, 45.0, 30.0]
        })
        
        result_df = format_to_database_schema(test_df)
        
        # Check column renaming
        self.assertIn('Site_Name', result_df.columns)
        self.assertIn('do_percent', result_df.columns)
        self.assertIn('Phosphorus', result_df.columns)
        
        # Check pH calculation (worst case)
        self.assertEqual(result_df['pH'].iloc[0], 7.5)  # 7.5 is further from 7 than 7.2
        self.assertEqual(result_df['pH'].iloc[1], 6.5)  # 6.5 is further from 7 than 6.8
        self.assertEqual(result_df['pH'].iloc[2], 8.0)  # Equidistant, should prefer pH #1
        
        # Check that soluble nitrogen was calculated
        self.assertIn('soluble_nitrogen', result_df.columns)
        
        # Check that all required columns are present
        required_columns = [
            'Site_Name', 'Date', 'Year', 'Month', 'do_percent', 'pH',
            'Nitrate', 'Nitrite', 'Ammonia', 'Phosphorus', 'Chloride',
            'soluble_nitrogen'
        ]
        for col in required_columns:
            self.assertIn(col, result_df.columns)

    # =============================================================================
    # NOTE: DUPLICATE HANDLING TESTS REMOVED
    # =============================================================================
    # Chemical duplicate consolidation functionality was removed from the project.
    # All replicate samples are now preserved as separate collection events,
    # maintaining complete data integrity for scientific analysis.

    # =============================================================================
    # INTEGRATION TESTS
    # =============================================================================

    @patch('data_processing.chemical_processing.save_processed_data')
    @patch('data_processing.chemical_processing.os.path.exists')
    @patch('data_processing.chemical_processing.pd.read_csv')
    def test_process_chemical_data_from_csv_basic(self, mock_read_csv, mock_exists, mock_save_data):
        """Test basic CSV processing functionality."""
        # Mock file existence
        mock_exists.return_value = True
        
        # Mock CSV reading
        mock_read_csv.return_value = self.sample_chemical_data
        
        # Mock save to prevent file writes
        mock_save_data.return_value = True
        
        result_df, key_params, ref_values = process_chemical_data_from_csv()
        
        # Verify mocks were called
        mock_read_csv.assert_called_once()
        mock_save_data.assert_called_once()
        
        # Check that data was processed
        self.assertFalse(result_df.empty)
        
        # Check that columns were renamed correctly
        expected_columns = ['Site_Name', 'Date', 'do_percent', 'pH', 'Nitrate', 
                          'Nitrite', 'Ammonia', 'Phosphorus', 'Chloride']
        for col in expected_columns:
            self.assertIn(col, result_df.columns)
        
        # Check that soluble_nitrogen was calculated
        self.assertIn('soluble_nitrogen', result_df.columns)
        
        # Check that key parameters were returned
        self.assertIsInstance(key_params, list)
        self.assertIn('do_percent', key_params)
        
        # Check that reference values were returned
        self.assertIsInstance(ref_values, dict)

    @patch('data_processing.updated_chemical_processing.load_updated_chemical_data')
    def test_updated_processing_pipeline(self, mock_load_data):
        """Test the complete updated chemical processing pipeline."""
        # Mock data loading
        mock_load_data.return_value = self.sample_updated_data.copy()
        
        # Process the data
        result_df = process_updated_chemical_data()
        
        # Check that data was processed
        self.assertFalse(result_df.empty)
        
        # Check that all required columns are present
        required_columns = [
            'Site_Name', 'Date', 'Year', 'Month', 'do_percent', 'pH',
            'Nitrate', 'Nitrite', 'Ammonia', 'Phosphorus', 'Chloride',
            'soluble_nitrogen'
        ]
        for col in required_columns:
            self.assertIn(col, result_df.columns)
        
        # Check that values were processed correctly
        self.assertEqual(result_df['do_percent'].iloc[0], 95.5)
        self.assertEqual(result_df['pH'].iloc[0], 7.3)  # pH #2 is further from 7 (7.3 vs 7.2)
        self.assertEqual(result_df['pH'].iloc[1], 8.1)  # pH #1 is further from 7 (8.1 vs 8.0)
        self.assertEqual(result_df['Nitrate'].iloc[0], 0.6)  # Greater of 0.5 and 0.6
        self.assertEqual(result_df['Nitrite'].iloc[1], 0.12)  # Greater of 0.1 and 0.12

    # =============================================================================
    # EDGE CASES
    # =============================================================================

    def test_edge_case_empty_data(self):
        """Test behavior with empty input data."""
        empty_df = pd.DataFrame()
        
        # Test individual functions with empty data
        result_bdl = apply_bdl_conversions(empty_df)
        self.assertTrue(result_bdl.empty)
        
        result_nitrogen = calculate_soluble_nitrogen(empty_df)
        self.assertTrue(result_nitrogen.empty)
        
        result_validation = validate_chemical_data(empty_df)
        self.assertTrue(result_validation.empty)

    def test_edge_case_all_null_values(self):
        """Test behavior with all null values."""
        null_df = pd.DataFrame({
            'Nitrate': [np.nan, np.nan],
            'Nitrite': [np.nan, np.nan],
            'Ammonia': [np.nan, np.nan]
        })
        
        # Should handle null values gracefully
        result_df = calculate_soluble_nitrogen(null_df)
        self.assertIn('soluble_nitrogen', result_df.columns)
        
        # Soluble nitrogen should be calculated using BDL values
        self.assertFalse(pd.isna(result_df.loc[0, 'soluble_nitrogen']))

if __name__ == '__main__':
    # Set up test discovery and run tests
    unittest.main(verbosity=2)