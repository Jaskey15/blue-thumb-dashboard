"""Tests for the API-first chemical data pipeline in arcgis_sync.py."""

import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock

import pandas as pd

from data_processing.arcgis_sync import (
    fetch_site_data,
    prepare_dataframe,
    parse_epoch_dates,
    process_fetched_data,
    resolve_unknown_sites,
    sync_all_chemical_data,
    _fetch_features_paginated,
)


class TestPrepareDataframe(unittest.TestCase):
    """Tests for prepare_dataframe (replaces translate_to_pipeline_schema)."""

    def test_normalizes_site_names(self):
        records = [{'SiteName': 'Coffee Creek:  N. Sooner Rd.', 'objectid': 1, 'QAQC_Complete': 'X'}]
        df = prepare_dataframe(records)
        self.assertEqual(df['SiteName'].iloc[0], 'Coffee Creek: N. Sooner Rd')

    def test_renames_objectid_to_sample_id(self):
        records = [{'SiteName': 'Test', 'objectid': 42, 'QAQC_Complete': 'X'}]
        df = prepare_dataframe(records)
        self.assertEqual(df['sample_id'].iloc[0], 42)

    def test_filters_qaqc_incomplete(self):
        records = [
            {'SiteName': 'A', 'objectid': 1, 'QAQC_Complete': 'X'},
            {'SiteName': 'B', 'objectid': 2, 'QAQC_Complete': None},
        ]
        df = prepare_dataframe(records)
        self.assertEqual(len(df), 1)

    def test_empty_records(self):
        df = prepare_dataframe([])
        self.assertTrue(df.empty)


class TestParseEpochDates(unittest.TestCase):
    """Tests for direct epoch ms → date conversion."""

    def test_converts_epoch_to_date(self):
        # 2025-03-15 in epoch ms (UTC)
        epoch_ms = 1742025600000
        df = pd.DataFrame({'day': [epoch_ms]})
        result = parse_epoch_dates(df)
        self.assertIn('Date', result.columns)
        self.assertIn('Year', result.columns)
        self.assertIn('Month', result.columns)
        self.assertEqual(result['Year'].iloc[0], 2025)
        self.assertEqual(result['Month'].iloc[0], 3)

    def test_handles_null_dates(self):
        df = pd.DataFrame({'day': [None]})
        result = parse_epoch_dates(df)
        self.assertTrue(pd.isna(result['Date'].iloc[0]))


class TestFetchSiteData(unittest.TestCase):
    """Tests for Feature Server site extraction."""

    @patch('data_processing.arcgis_sync._fetch_features_paginated')
    def test_extracts_sites_with_geometry(self, mock_fetch):
        mock_fetch.return_value = [
            {
                'attributes': {'SiteName': 'Wolf Creek: Gore Blvd.', 'CountyName': 'Comanche'},
                'geometry': {'x': -98.44398, 'y': 34.60876},
            },
            {
                'attributes': {'SiteName': 'Coal Creek: Hwy 11', 'CountyName': 'Tulsa'},
                'geometry': {'x': -95.914999, 'y': 36.195556},
            },
        ]
        df = fetch_site_data()
        self.assertEqual(len(df), 2)
        self.assertAlmostEqual(df.iloc[0]['latitude'], 34.60876)
        self.assertAlmostEqual(df.iloc[0]['longitude'], -98.44398)
        self.assertEqual(df.iloc[0]['county'], 'Comanche')

    @patch('data_processing.arcgis_sync._fetch_features_paginated')
    def test_deduplicates_by_site_name(self, mock_fetch):
        mock_fetch.return_value = [
            {
                'attributes': {'SiteName': 'Same Site', 'CountyName': 'Tulsa'},
                'geometry': {'x': -95.9, 'y': 36.2},
            },
            {
                'attributes': {'SiteName': 'Same Site', 'CountyName': 'Tulsa'},
                'geometry': {'x': -95.9, 'y': 36.2},
            },
        ]
        df = fetch_site_data()
        self.assertEqual(len(df), 1)

    @patch('data_processing.arcgis_sync._fetch_features_paginated')
    def test_empty_response(self, mock_fetch):
        mock_fetch.return_value = []
        df = fetch_site_data()
        self.assertTrue(df.empty)


class TestPagination(unittest.TestCase):
    """Tests for Feature Server pagination."""

    @patch('data_processing.arcgis_sync.requests.get')
    def test_paginates_on_exceeded_transfer_limit(self, mock_get):
        page1 = MagicMock()
        page1.json.return_value = {
            'features': [{'attributes': {'objectid': i}} for i in range(2000)],
            'exceededTransferLimit': True,
        }
        page1.raise_for_status = MagicMock()

        page2 = MagicMock()
        page2.json.return_value = {
            'features': [{'attributes': {'objectid': i}} for i in range(2000, 2500)],
        }
        page2.raise_for_status = MagicMock()

        mock_get.side_effect = [page1, page2]

        records = _fetch_features_paginated(
            where="1=1", out_fields=['objectid'], order_by_fields='objectid ASC'
        )
        self.assertEqual(len(records), 2500)


class TestSyncAllChemicalData(unittest.TestCase):
    """Tests for the full-fetch entry point."""

    @patch('data_processing.arcgis_sync.insert_chemical_data')
    @patch('data_processing.arcgis_sync.filter_known_sites')
    @patch('data_processing.arcgis_sync.process_fetched_data')
    @patch('data_processing.arcgis_sync.prepare_dataframe')
    @patch('data_processing.arcgis_sync._fetch_features_paginated')
    def test_full_sync_pipeline(self, mock_fetch, mock_prepare, mock_process, mock_filter, mock_insert):
        mock_fetch.return_value = [{'objectid': 1}]
        mock_prepare.return_value = pd.DataFrame({'Site_Name': ['Test'], 'Date': ['2025-01-01']})
        mock_process.return_value = pd.DataFrame({'Site_Name': ['Test'], 'Date': ['2025-01-01']})
        mock_filter.return_value = (pd.DataFrame({'Site_Name': ['Test'], 'Date': ['2025-01-01']}), [])
        mock_insert.return_value = {'measurements_added': 1, 'events_added': 1, 'sites_processed': 1}

        result = sync_all_chemical_data()
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['records_inserted'], 1)
        mock_fetch.assert_called_once()

    @patch('data_processing.arcgis_sync._fetch_features_paginated')
    def test_empty_fetch_returns_zero(self, mock_fetch):
        mock_fetch.return_value = []
        result = sync_all_chemical_data()
        self.assertEqual(result['records_fetched'], 0)
        self.assertEqual(result['records_inserted'], 0)


class TestProcessFetchedDataIntegration(unittest.TestCase):
    """Integration test: API-format records → DB-ready DataFrame."""

    def test_end_to_end_processing(self):
        """Verify a realistic API record processes to correct DB columns."""
        records = [{
            'objectid': 3857,
            'SiteName': 'Fisher Creek: Hwy 51',
            'day': 1742025600000,  # 2025-03-15
            'oxygen_sat': 95.0,
            'pH1': 7.8,
            'pH2': 7.2,
            'nitratetest1': 1.5,
            'nitratetest2': 1.2,
            'nitritetest1': 0.05,
            'nitritetest2': 0.03,
            'Ammonia_Range': 'Low Range',
            'ammonia_Nitrogen2': 0.1,
            'ammonia_Nitrogen3': 0.08,
            'Ammonia_nitrogen_midrange1_Final': None,
            'Ammonia_nitrogen_midrange2_Final': None,
            'Ortho_Range': 'Low Range',
            'Orthophosphate_Low1_Final': 0.02,
            'Orthophosphate_Low2_Final': 0.01,
            'Orthophosphate_Mid1_Final': None,
            'Orthophosphate_Mid2_Final': None,
            'Orthophosphate_High1_Final': None,
            'Orthophosphate_High2_Final': None,
            'Chloride_Range': 'Low Range',
            'Chloride_Low1_Final': 15.0,
            'Chloride_Low2_Final': 14.0,
            'Chloride_High1_Final': None,
            'Chloride_High2_Final': None,
            'QAQC_Complete': 'X',
        }]

        df = prepare_dataframe(records)
        result = process_fetched_data(df)

        self.assertEqual(len(result), 1)
        row = result.iloc[0]
        self.assertEqual(row['Site_Name'], 'Fisher Creek: Hwy 51')
        self.assertEqual(row['do_percent'], 95.0)
        # pH worst-case: 7.8 is further from 7 (0.8) than 7.2 (0.2)
        self.assertEqual(row['pH'], 7.8)
        self.assertEqual(row['Nitrate'], 1.5)  # greater of 1.5, 1.2
        self.assertEqual(row['Nitrite'], 0.05)  # greater of 0.05, 0.03
        self.assertEqual(row['Ammonia'], 0.1)  # low range, greater of 0.1, 0.08
        self.assertEqual(row['Phosphorus'], 0.02)  # low range, greater of 0.02, 0.01
        self.assertEqual(row['Chloride'], 15.0)  # low range, greater of 15, 14
        self.assertIn('soluble_nitrogen', result.columns)
        self.assertIn('sample_id', result.columns)


class TestResolveUnknownSites(unittest.TestCase):
    """Tests for resolve_unknown_sites resolution chain."""

    import sqlite3

    def _make_db(self):
        """Create an in-memory DB with test sites."""
        conn = self.sqlite3.connect(':memory:')
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('''
            CREATE TABLE sites (
                site_id INTEGER PRIMARY KEY AUTOINCREMENT,
                site_name TEXT NOT NULL UNIQUE,
                latitude REAL,
                longitude REAL,
                active INTEGER DEFAULT 1,
                source_file TEXT
            )
        ''')
        conn.execute(
            "INSERT INTO sites (site_name, latitude, longitude) VALUES (?, ?, ?)",
            ('Coffee Creek: N. Sooner Rd', 35.5, -97.5),
        )
        conn.execute(
            "INSERT INTO sites (site_name, latitude, longitude) VALUES (?, ?, ?)",
            ('Boomer Creek: 3rd Ave', 36.1, -97.1),
        )
        conn.commit()
        return conn

    def test_exact_match_keeps_row(self):
        """Site that exactly matches DB is kept, stats show already_known=1."""
        conn = self._make_db()
        df = pd.DataFrame({
            'Site_Name': ['Coffee Creek: N. Sooner Rd'],
            'latitude': [35.5],
            'longitude': [-97.5],
        })
        resolved_df, stats = resolve_unknown_sites(df, conn)
        self.assertEqual(len(resolved_df), 1)
        self.assertEqual(resolved_df['Site_Name'].iloc[0], 'Coffee Creek: N. Sooner Rd')
        self.assertEqual(stats['already_known'], 1)
        conn.close()

    def test_normalized_match_resolves(self):
        """Site name differing by whitespace/punctuation resolves to DB name."""
        conn = self._make_db()
        df = pd.DataFrame({
            'Site_Name': ['Coffee Creek:  N. Sooner Rd.'],
            'latitude': [35.5],
            'longitude': [-97.5],
        })
        resolved_df, stats = resolve_unknown_sites(df, conn)
        self.assertEqual(len(resolved_df), 1)
        self.assertEqual(resolved_df['Site_Name'].iloc[0], 'Coffee Creek: N. Sooner Rd')
        self.assertEqual(stats['normalized_match'], 1)
        conn.close()

    def test_haversine_match_resolves(self):
        """Site within 50m of existing site resolves via coordinates."""
        conn = self._make_db()
        # Offset by ~30m (well within 50m threshold)
        df = pd.DataFrame({
            'Site_Name': ['Unknown Nearby Site'],
            'latitude': [35.50027],
            'longitude': [-97.5],
        })
        resolved_df, stats = resolve_unknown_sites(df, conn)
        self.assertEqual(len(resolved_df), 1)
        self.assertEqual(resolved_df['Site_Name'].iloc[0], 'Coffee Creek: N. Sooner Rd')
        self.assertEqual(stats['coordinate_match'], 1)
        conn.close()

    def test_auto_insert_creates_new_site(self):
        """Genuinely new site is auto-inserted into sites table."""
        conn = self._make_db()
        df = pd.DataFrame({
            'Site_Name': ['Brand New Creek: Hwy 99'],
            'latitude': [34.0],
            'longitude': [-96.0],
        })
        resolved_df, stats = resolve_unknown_sites(df, conn)
        self.assertEqual(len(resolved_df), 1)
        self.assertEqual(resolved_df['Site_Name'].iloc[0], 'Brand New Creek: Hwy 99')
        self.assertEqual(stats['auto_inserted'], 1)
        # Verify actually in DB
        row = conn.execute(
            "SELECT site_name FROM sites WHERE site_name = ?",
            ('Brand New Creek: Hwy 99',),
        ).fetchone()
        self.assertIsNotNone(row)
        conn.close()

    def test_no_rows_dropped(self):
        """Mix of known, normalized, and new sites — all 3 rows preserved."""
        conn = self._make_db()
        df = pd.DataFrame({
            'Site_Name': [
                'Coffee Creek: N. Sooner Rd',       # exact match
                'Boomer Creek:  3rd Ave.',           # normalized match
                'Totally New Creek: Main St',        # auto-insert
            ],
            'latitude': [35.5, 36.1, 34.0],
            'longitude': [-97.5, -97.1, -96.0],
        })
        resolved_df, stats = resolve_unknown_sites(df, conn)
        self.assertEqual(len(resolved_df), 3)
        self.assertEqual(stats['already_known'], 1)
        self.assertEqual(stats['normalized_match'], 1)
        self.assertEqual(stats['auto_inserted'], 1)
        conn.close()
