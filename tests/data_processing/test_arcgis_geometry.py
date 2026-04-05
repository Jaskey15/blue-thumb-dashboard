"""Tests for geometry extraction from FeatureServer responses."""
import unittest
from unittest.mock import patch, MagicMock
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from data_processing.arcgis_sync import _fetch_features_paginated


class TestGeometryExtraction(unittest.TestCase):
    """Verify geometry handling in _fetch_features_paginated."""

    @patch('data_processing.arcgis_sync.requests.get')
    def test_return_geometry_true_returns_full_feature_dicts(self, mock_get):
        """With return_geometry=True, records should be full feature dicts."""
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
            return_geometry=True,
        )

        self.assertEqual(len(records), 1)
        # Full feature dict preserved
        self.assertIn('attributes', records[0])
        self.assertIn('geometry', records[0])
        self.assertEqual(records[0]['geometry']['x'], -97.5)
        self.assertEqual(records[0]['geometry']['y'], 35.4)

    @patch('data_processing.arcgis_sync.requests.get')
    def test_return_geometry_false_returns_attribute_dicts(self, mock_get):
        """With return_geometry=False (default), records are flat attribute dicts."""
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
        self.assertEqual(records[0]['objectid'], 2)
        self.assertNotIn('attributes', records[0])

    @patch('data_processing.arcgis_sync.requests.get')
    def test_return_geometry_param_sent_in_request(self, mock_get):
        """The returnGeometry param should match the argument."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'features': []}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        _fetch_features_paginated(
            where="1=1",
            out_fields=['objectid'],
            order_by_fields='objectid',
            return_geometry=True,
        )

        call_args = mock_get.call_args
        params = call_args.kwargs.get('params') or call_args[1].get('params')
        self.assertEqual(params.get('returnGeometry'), True)


if __name__ == '__main__':
    unittest.main()
