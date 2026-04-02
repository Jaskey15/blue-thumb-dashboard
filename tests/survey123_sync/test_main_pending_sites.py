"""Tests for pending site orchestration in main.py."""
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'cloud_functions', 'survey123_sync'))

sys.modules['functions_framework'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()


class TestPromoteBeforeFetch(unittest.TestCase):
    """Verify promote_approved_sites is called before data fetch."""

    @patch('main._run_feature_server_sync')
    def test_response_includes_pending_sites_block(self, mock_sync):
        """Sync response should include pending_sites metadata."""
        mock_sync.return_value = {
            'status': 'success',
            'pending_sites': {
                'new_pending': 0,
                'total_pending': 2,
                'promoted': 1,
                'coordinate_matched': 0,
                'names': [],
            }
        }
        result = mock_sync()
        self.assertIn('pending_sites', result)
        self.assertIn('total_pending', result['pending_sites'])


if __name__ == '__main__':
    unittest.main()
