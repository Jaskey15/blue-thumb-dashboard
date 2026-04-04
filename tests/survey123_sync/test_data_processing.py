"""
Tests for FeatureServer sync behavior.
"""

import os
import sys
import sqlite3
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

import main
import chemical_processor


class TestSyncModeBehavior(unittest.TestCase):
    """Executable proof for feature_server sync routing."""

    @patch('main.os.unlink')
    @patch('main.tempfile.NamedTemporaryFile')
    @patch('main.datetime')
    def test_run_feature_server_sync_no_records_updates_metadata(self, mock_datetime, mock_temp_file, mock_unlink):
        start_time = pd.Timestamp('2026-02-18T00:00:00').to_pydatetime()
        mock_datetime.now.return_value = start_time

        temp_ctx = MagicMock()
        temp_ctx.__enter__.return_value.name = '/tmp/test-sync.db'
        temp_ctx.__exit__.return_value = False
        mock_temp_file.return_value = temp_ctx

        db_manager = MagicMock()
        db_manager.download_database.return_value = True
        db_manager.bucket.blob.return_value.exists.return_value = False

        with patch('main._get_db_latest_chemical_date', return_value='2020-01-01'), \
             patch('data_processing.arcgis_sync.fetch_features_since', return_value=[]):
            result = main._run_feature_server_sync(db_manager, start_time)

        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['mode'], 'feature_server')
        self.assertEqual(result['records_fetched'], 0)
        self.assertEqual(result['sync_strategy'], 'day')
        db_manager.update_sync_timestamp.assert_called_once()
        mock_unlink.assert_called_once_with('/tmp/test-sync.db')


class TestFeatureServerSiteResolution(unittest.TestCase):
    def _create_minimal_db(self) -> str:
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)

        conn = sqlite3.connect(path)
        cur = conn.cursor()

        cur.execute("PRAGMA foreign_keys = ON")

        cur.execute(
            """
            CREATE TABLE sites (
                site_id INTEGER PRIMARY KEY,
                site_name TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                county TEXT,
                river_basin TEXT,
                ecoregion TEXT,
                active BOOLEAN DEFAULT 1,
                last_chemical_reading_date TEXT,
                UNIQUE(site_name)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE chemical_collection_events (
                event_id INTEGER PRIMARY KEY,
                site_id INTEGER,
                sample_id INTEGER,
                collection_date TEXT,
                year INTEGER,
                month INTEGER,
                FOREIGN KEY (site_id) REFERENCES sites(site_id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE chemical_measurements (
                event_id INTEGER,
                parameter_id INTEGER,
                value REAL,
                status TEXT,
                PRIMARY KEY (event_id, parameter_id)
            )
            """
        )

        conn.commit()
        conn.close()
        return path

    def test_site_alias_resolves_to_existing_site(self):
        db_path = self._create_minimal_db()
        try:
            canonical = 'Cow Creek: West Virginia Avenue'
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO sites (site_name, latitude, longitude) VALUES (?, ?, ?)",
                (canonical, 36.12318, -97.09975),
            )
            site_id = cur.lastrowid
            conn.commit()
            conn.close()

            df = pd.DataFrame(
                [
                    {
                        'Site_Name': 'Cow Creek: Virginia Avenue',
                        'Date': pd.Timestamp('2026-02-18'),
                        'Year': 2026,
                        'Month': 2,
                        'pH': 7.1,
                        'sample_id': 1001,
                    }
                ]
            )

            with patch('chemical_processor.get_reference_values_from_db', return_value={}), patch(
                'chemical_processor.determine_status', return_value='Normal'
            ):
                result = chemical_processor.insert_processed_data_to_db(df, db_path)

            self.assertEqual(result['records_inserted'], 1)
            self.assertEqual(result['skipped_records_unknown_sites'], 0)
            self.assertEqual(result['unknown_sites'], [])

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT site_id FROM chemical_collection_events")
            rows = cur.fetchall()
            conn.close()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], site_id)
        finally:
            os.unlink(db_path)

    def test_site_alias_blue_beaver_cache_rd(self):
        db_path = self._create_minimal_db()
        try:
            canonical = 'Blue Beaver Creek: Pecan Road'
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO sites (site_name, latitude, longitude) VALUES (?, ?, ?)",
                (canonical, 34.5361666, -98.5655277),
            )
            site_id = cur.lastrowid
            conn.commit()
            conn.close()

            df = pd.DataFrame(
                [
                    {
                        'Site_Name': 'Blue Beaver Creek: Cache Rd',
                        'Date': pd.Timestamp('2026-02-18'),
                        'Year': 2026,
                        'Month': 2,
                        'pH': 7.1,
                        'sample_id': 2001,
                    }
                ]
            )

            with patch('chemical_processor.get_reference_values_from_db', return_value={}), patch(
                'chemical_processor.determine_status', return_value='Normal'
            ):
                result = chemical_processor.insert_processed_data_to_db(df, db_path)

            self.assertEqual(result['records_inserted'], 1)
            self.assertEqual(result['skipped_records_unknown_sites'], 0)
            self.assertEqual(result['unknown_sites'], [])

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT site_id FROM chemical_collection_events")
            rows = cur.fetchall()
            conn.close()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], site_id)
        finally:
            os.unlink(db_path)

    def test_site_alias_mooser_riverfield(self):
        db_path = self._create_minimal_db()
        try:
            canonical = 'Mooser Creek Trib: Riverfield School'
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO sites (site_name, latitude, longitude) VALUES (?, ?, ?)",
                (canonical, 36.0792, -96.0184),
            )
            site_id = cur.lastrowid
            conn.commit()
            conn.close()

            df = pd.DataFrame(
                [
                    {
                        'Site_Name': 'Mooser Creek: Riverfield',
                        'Date': pd.Timestamp('2026-02-18'),
                        'Year': 2026,
                        'Month': 2,
                        'pH': 7.1,
                        'sample_id': 2002,
                    }
                ]
            )

            with patch('chemical_processor.get_reference_values_from_db', return_value={}), patch(
                'chemical_processor.determine_status', return_value='Normal'
            ):
                result = chemical_processor.insert_processed_data_to_db(df, db_path)

            self.assertEqual(result['records_inserted'], 1)
            self.assertEqual(result['skipped_records_unknown_sites'], 0)
            self.assertEqual(result['unknown_sites'], [])

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT site_id FROM chemical_collection_events")
            rows = cur.fetchall()
            conn.close()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], site_id)
        finally:
            os.unlink(db_path)

    def test_site_alias_deep_fork_canyon_park(self):
        db_path = self._create_minimal_db()
        try:
            canonical = 'Deep Fork Tributary: Classen'
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO sites (site_name, latitude, longitude) VALUES (?, ?, ?)",
                (canonical, 35.53349, -97.528907),
            )
            site_id = cur.lastrowid
            conn.commit()
            conn.close()

            df = pd.DataFrame(
                [
                    {
                        'Site_Name': 'Deep Fork River: Canyon Park',
                        'Date': pd.Timestamp('2026-02-18'),
                        'Year': 2026,
                        'Month': 2,
                        'pH': 7.1,
                        'sample_id': 2003,
                    }
                ]
            )

            with patch('chemical_processor.get_reference_values_from_db', return_value={}), patch(
                'chemical_processor.determine_status', return_value='Normal'
            ):
                result = chemical_processor.insert_processed_data_to_db(df, db_path)

            self.assertEqual(result['records_inserted'], 1)
            self.assertEqual(result['skipped_records_unknown_sites'], 0)
            self.assertEqual(result['unknown_sites'], [])

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT site_id FROM chemical_collection_events")
            rows = cur.fetchall()
            conn.close()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], site_id)
        finally:
            os.unlink(db_path)

    def test_site_alias_arkansas_trib_walton(self):
        db_path = self._create_minimal_db()
        try:
            canonical = 'Unknown Trib to Arkansas River'
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO sites (site_name, latitude, longitude) VALUES (?, ?, ?)",
                (canonical, 35.75839, -95.30722),
            )
            site_id = cur.lastrowid
            conn.commit()
            conn.close()

            df = pd.DataFrame(
                [
                    {
                        'Site_Name': 'Tributary to Arkansas River: Walton',
                        'Date': pd.Timestamp('2026-02-18'),
                        'Year': 2026,
                        'Month': 2,
                        'pH': 7.1,
                        'sample_id': 2004,
                    }
                ]
            )

            with patch('chemical_processor.get_reference_values_from_db', return_value={}), patch(
                'chemical_processor.determine_status', return_value='Normal'
            ):
                result = chemical_processor.insert_processed_data_to_db(df, db_path)

            self.assertEqual(result['records_inserted'], 1)
            self.assertEqual(result['skipped_records_unknown_sites'], 0)
            self.assertEqual(result['unknown_sites'], [])

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT site_id FROM chemical_collection_events")
            rows = cur.fetchall()
            conn.close()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], site_id)
        finally:
            os.unlink(db_path)

    def test_site_normalization_matches_trailing_period(self):
        db_path = self._create_minimal_db()
        try:
            canonical = 'North Fork of Little River: SE 34th St.'
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO sites (site_name, latitude, longitude) VALUES (?, ?, ?)",
                (canonical, 35.305315, -97.445216),
            )
            site_id = cur.lastrowid
            conn.commit()
            conn.close()

            df = pd.DataFrame(
                [
                    {
                        'Site_Name': 'North Fork of Little River: SE 34th St',
                        'Date': pd.Timestamp('2026-02-18'),
                        'Year': 2026,
                        'Month': 2,
                        'pH': 7.3,
                        'sample_id': 1002,
                    }
                ]
            )

            with patch('chemical_processor.get_reference_values_from_db', return_value={}), patch(
                'chemical_processor.determine_status', return_value='Normal'
            ):
                result = chemical_processor.insert_processed_data_to_db(df, db_path)

            self.assertEqual(result['records_inserted'], 1)
            self.assertEqual(result['skipped_records_unknown_sites'], 0)
            self.assertEqual(result['unknown_sites'], [])

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT site_id FROM chemical_collection_events")
            rows = cur.fetchall()
            conn.close()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], site_id)
        finally:
            os.unlink(db_path)

    def test_unknown_site_is_skipped_and_reported(self):
        db_path = self._create_minimal_db()
        try:
            df = pd.DataFrame(
                [
                    {
                        'Site_Name': 'Definitely Not A Real Site',
                        'Date': pd.Timestamp('2026-02-18'),
                        'Year': 2026,
                        'Month': 2,
                        'pH': 7.3,
                        'sample_id': 9999,
                    }
                ]
            )

            with patch('chemical_processor.get_reference_values_from_db', return_value={}), patch(
                'chemical_processor.determine_status', return_value='Normal'
            ):
                result = chemical_processor.insert_processed_data_to_db(df, db_path)

            self.assertEqual(result['records_inserted'], 0)
            self.assertEqual(result['skipped_records_unknown_sites'], 1)
            self.assertEqual(result['unknown_sites'], ['Definitely Not A Real Site'])
            self.assertEqual(result['unknown_site_counts'], {'Definitely Not A Real Site': 1})
            self.assertEqual(result['unknown_site_sample_ids'], {'Definitely Not A Real Site': [9999]})
            self.assertEqual(result['unknown_site_sample_ids_truncated'], False)
            self.assertEqual(result['unknown_site_sample_ids_limit_per_site'], 50)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM chemical_collection_events")
            count = cur.fetchone()[0]
            conn.close()
            self.assertEqual(count, 0)
        finally:
            os.unlink(db_path)

    @patch('main.os.unlink')
    @patch('main.tempfile.NamedTemporaryFile')
    @patch('main.datetime')
    def test_run_feature_server_sync_processes_and_uploads(self, mock_datetime, mock_temp_file, mock_unlink):
        start_time = pd.Timestamp('2026-02-18T00:00:00').to_pydatetime()
        mock_datetime.now.return_value = start_time

        temp_ctx = MagicMock()
        temp_ctx.__enter__.return_value.name = '/tmp/test-sync.db'
        temp_ctx.__exit__.return_value = False
        mock_temp_file.return_value = temp_ctx

        db_manager = MagicMock()
        db_manager.download_database.return_value = True
        db_manager.upload_database.return_value = True
        db_manager.bucket.blob.return_value.exists.return_value = True
        db_manager.get_last_sync_timestamp.return_value = start_time

        records = [{'objectid': 1}]
        processed = pd.DataFrame([{'sample_id': 1, 'Site_Name': 'A'}])

        with patch('data_processing.arcgis_sync.fetch_features_edited_since', return_value=records), \
             patch('data_processing.arcgis_sync.prepare_dataframe', return_value=pd.DataFrame(records)), \
             patch('data_processing.arcgis_sync.process_fetched_data', return_value=processed), \
             patch('chemical_processor.insert_processed_data_to_db', return_value={'records_inserted': 1}), \
             patch('chemical_processor.classify_active_sites_in_db', return_value={'sites_classified': 1, 'active_count': 1, 'historic_count': 0}):
            result = main._run_feature_server_sync(db_manager, start_time)

        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['mode'], 'feature_server')
        self.assertEqual(result['records_fetched'], 1)
        self.assertEqual(result['records_processed'], 1)
        self.assertEqual(result['records_inserted'], 1)
        self.assertEqual(result['sync_strategy'], 'editdate')
        self.assertIn('site_classification', result)
        db_manager.upload_database.assert_called_once_with('/tmp/test-sync.db')
        db_manager.update_sync_timestamp.assert_called_once()
        mock_unlink.assert_called_once_with('/tmp/test-sync.db')


if __name__ == '__main__':
    unittest.main() 