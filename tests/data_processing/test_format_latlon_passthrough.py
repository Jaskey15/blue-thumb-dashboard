"""Tests that lat/lon columns survive format_to_database_schema in arcgis_sync."""
import unittest
import os
import sys

import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from data_processing.arcgis_sync import format_to_database_schema


class TestFormatLatLonPassthrough(unittest.TestCase):
    """Verify latitude/longitude columns are preserved through formatting."""

    def _make_sample_df(self, include_coords=True):
        """Create a minimal DataFrame matching arcgis_sync's format_to_database_schema input.

        The function expects a DataFrame that has already been through
        process_fetched_data's earlier stages (parse_epoch_dates, process_simple_nutrients,
        etc.). So we provide post-processed column names: Date/Year/Month (not 'day'),
        and API field names that COLUMN_TO_DB will rename (SiteName, oxygen_sat,
        Orthophosphate). get_ph_worst_case needs pH1 and pH2 columns.
        """
        data = {
            'SiteName': ['Test Creek'],
            'Date': [pd.Timestamp('2026-01-15')],
            'Year': [2026],
            'Month': [1],
            'oxygen_sat': [95.0],
            'pH1': [7.2],
            'pH2': [7.3],
            'Nitrate': [1.0],
            'Nitrite': [0.1],
            'Ammonia': [0.5],
            'Orthophosphate': [0.05],
            'Chloride': [25.0],
            'sample_id': [123],
        }
        if include_coords:
            data['latitude'] = [35.4]
            data['longitude'] = [-97.5]
        return pd.DataFrame(data)

    def test_latlon_preserved_when_present(self):
        """latitude and longitude should survive format_to_database_schema."""
        df = self._make_sample_df(include_coords=True)
        result = format_to_database_schema(df)
        self.assertIn('latitude', result.columns)
        self.assertIn('longitude', result.columns)
        self.assertAlmostEqual(result.iloc[0]['latitude'], 35.4)
        self.assertAlmostEqual(result.iloc[0]['longitude'], -97.5)

    def test_no_latlon_still_works(self):
        """Pipeline should still work when lat/lon columns are absent."""
        df = self._make_sample_df(include_coords=False)
        result = format_to_database_schema(df)
        self.assertNotIn('latitude', result.columns)
        self.assertNotIn('longitude', result.columns)
        self.assertGreater(len(result), 0)


if __name__ == '__main__':
    unittest.main()
