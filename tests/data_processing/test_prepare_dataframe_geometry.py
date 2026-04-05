"""Tests that prepare_dataframe extracts geometry into lat/lon columns."""
import unittest
import os
import sys

import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from data_processing.arcgis_sync import prepare_dataframe


class TestPrepareDataframeGeometry(unittest.TestCase):
    """Verify lat/lon extraction in prepare_dataframe."""

    def test_geometry_extracted_to_columns(self):
        """Full feature dicts should get latitude/longitude columns."""
        records = [
            {
                'attributes': {
                    'objectid': 1,
                    'SiteName': 'Test Creek',
                    'QAQC_Complete': 'Yes',
                },
                'geometry': {'x': -97.5, 'y': 35.4},
            },
        ]
        df = prepare_dataframe(records)
        self.assertIn('latitude', df.columns)
        self.assertIn('longitude', df.columns)
        self.assertAlmostEqual(df.iloc[0]['latitude'], 35.4)
        self.assertAlmostEqual(df.iloc[0]['longitude'], -97.5)

    def test_missing_geometry_gives_none(self):
        """Records without geometry should have None for lat/lon."""
        records = [
            {
                'attributes': {
                    'objectid': 2,
                    'SiteName': 'No Geo Creek',
                    'QAQC_Complete': 'Yes',
                },
            },
        ]
        df = prepare_dataframe(records)
        self.assertIn('latitude', df.columns)
        self.assertIsNone(df.iloc[0]['latitude'])

    def test_flat_attribute_dicts_still_work(self):
        """Attribute-only dicts (no 'attributes' key) should still parse."""
        records = [
            {
                'objectid': 3,
                'SiteName': 'Flat Creek',
                'QAQC_Complete': 'Yes',
            },
        ]
        df = prepare_dataframe(records)
        self.assertIn('sample_id', df.columns)
        self.assertEqual(df.iloc[0]['sample_id'], 3)

    def test_objectid_renamed_to_sample_id(self):
        """objectid column should be renamed to sample_id."""
        records = [
            {
                'attributes': {
                    'objectid': 10,
                    'SiteName': 'Rename Creek',
                    'QAQC_Complete': 'Yes',
                },
                'geometry': {'x': -97.0, 'y': 35.0},
            },
        ]
        df = prepare_dataframe(records)
        self.assertIn('sample_id', df.columns)
        self.assertNotIn('objectid', df.columns)


if __name__ == '__main__':
    unittest.main()
