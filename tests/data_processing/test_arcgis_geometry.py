"""Tests for geometry extraction from FeatureServer responses."""
import unittest
from unittest.mock import patch, MagicMock
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from data_processing.arcgis_sync import _fetch_features_paginated


class TestGeometryExtraction(unittest.TestCase):
    """Verify lat/lon are extracted from FeatureServer geometry."""

    @patch('data_processing.arcgis_sync.requests.get')
    def test_geometry_extracted_into_attributes(self, mock_get):
        """Geometry x/y should be injected as longitude/latitude in records."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'features': [
                {
                    'attributes': {'objectid': 1, 'SiteName': 'Test Creek'},
                    'geometry': {'x': -97.5, 'y': 35.4},
                },
            ],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        records = _fetch_features_paginated(
            where="1=1",
            out_fields=['objectid', 'SiteName'],
            order_by_fields='objectid',
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['longitude'], -97.5)
        self.assertEqual(records[0]['latitude'], 35.4)

    @patch('data_processing.arcgis_sync.requests.get')
    def test_missing_geometry_gives_none(self, mock_get):
        """Records without geometry should have latitude/longitude as None."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'features': [
                {
                    'attributes': {'objectid': 2, 'SiteName': 'No Geo Creek'},
                },
            ],
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        records = _fetch_features_paginated(
            where="1=1",
            out_fields=['objectid', 'SiteName'],
            order_by_fields='objectid',
        )

        self.assertEqual(len(records), 1)
        self.assertIsNone(records[0]['latitude'])
        self.assertIsNone(records[0]['longitude'])

    @patch('data_processing.arcgis_sync.requests.get')
    def test_return_geometry_param_sent(self, mock_get):
        """The returnGeometry param should be included in the request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'features': []}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        _fetch_features_paginated(
            where="1=1",
            out_fields=['objectid'],
            order_by_fields='objectid',
        )

        call_args = mock_get.call_args
        params = call_args.kwargs.get('params') or call_args[1].get('params')
        self.assertEqual(params.get('returnGeometry'), 'true')


if __name__ == '__main__':
    unittest.main()
