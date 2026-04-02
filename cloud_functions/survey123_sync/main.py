"""
FeatureServer Daily Sync Cloud Function

Automated daily synchronization of chemical data from the public ArcGIS FeatureServer
with the Blue Thumb Dashboard. Updates SQLite database in Cloud Storage.

NOTE: Entry point name 'survey123_daily_sync' is legacy — retained for GCP config compatibility.
TODO: Rename to 'data_sync' and update GCP function config.

Environment Variables:
- GOOGLE_CLOUD_PROJECT: GCP project ID
- GCS_BUCKET_DATABASE: Cloud Storage bucket for database
"""

import json
import logging
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Optional

import functions_framework
import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_candidate_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)
)
if (
    os.path.isdir(os.path.join(_candidate_root, 'data_processing'))
    and os.path.isdir(os.path.join(_candidate_root, 'database'))
    and _candidate_root not in sys.path
):
    sys.path.insert(0, _candidate_root)

# Environment configuration
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT')
DATABASE_BUCKET = os.environ.get('GCS_BUCKET_DATABASE', 'blue-thumb-database')


class DatabaseManager:
    """Manage SQLite database operations in Cloud Storage with backup handling."""
    
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.db_blob_name = os.environ.get('GCS_DB_BLOB_NAME', 'blue_thumb.db')
    
    def download_database(self, local_path: str) -> bool:
        """Download database from Cloud Storage for local processing."""
        try:
            blob = self.bucket.blob(self.db_blob_name)
            if not blob.exists():
                logger.error(f"Database {self.db_blob_name} not found in bucket {self.bucket.name}")
                return False
            
            blob.download_to_filename(local_path)
            logger.info(f"Downloaded database to {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading database: {e}")
            return False
    
    def upload_database(self, local_path: str) -> bool:
        """Upload updated database with automatic backup creation."""
        bucket_name = getattr(self.bucket, 'name', None) or '<unknown-bucket>'
        db_object_name = self.db_blob_name

        try:
            backup_name = (
                f"backups/blue_thumb_backup_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')}.db"
            )
            blob = self.bucket.blob(db_object_name)

            if blob.exists():
                try:
                    backup_blob = self.bucket.blob(backup_name)
                    backup_blob.upload_from_string(blob.download_as_string())
                    logger.info(f"Created backup: gs://{bucket_name}/{backup_name}")
                except Exception as e:
                    logger.error(
                        f"Error creating backup: gs://{bucket_name}/{backup_name} from gs://{bucket_name}/{db_object_name}: {e}"
                    )
                    # Proceed with primary upload even if backup fails
            else:
                logger.info(
                    f"No existing database object found at gs://{bucket_name}/{db_object_name}; skipping backup"
                )

            try:
                new_blob = self.bucket.blob(db_object_name)
                new_blob.upload_from_filename(local_path)
                logger.info(f"Uploaded updated database to gs://{bucket_name}/{db_object_name}")
                return True
            except Exception as e:
                logger.error(
                    f"Error uploading updated database to gs://{bucket_name}/{db_object_name} from {local_path}: {e}"
                )
                raise

        except Exception as e:
            logger.error(
                f"Error uploading database (bucket={bucket_name} object={db_object_name}): {e}"
            )
            return False
    
    def get_last_sync_timestamp(self, metadata_blob_name: str = 'sync_metadata/last_sync.json') -> datetime:
        """Get timestamp of last successful sync for incremental updates."""
        try:
            blob = self.bucket.blob(metadata_blob_name)
            if blob.exists():
                metadata = json.loads(blob.download_as_string())
                ts = datetime.fromisoformat(metadata['last_sync_timestamp'])
                if ts.tzinfo is not None:
                    ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
                return ts
            else:
                return datetime.now() - timedelta(days=7)  # Default lookback for first run
                
        except Exception as e:
            logger.warning(f"Error reading last sync timestamp: {e}")
            return datetime.now() - timedelta(days=7)
    
    def update_sync_timestamp(
        self,
        timestamp: datetime,
        metadata_blob_name: str = 'sync_metadata/last_sync.json',
        metadata_extra: Optional[dict] = None,
    ) -> bool:
        """Record successful sync timestamp for next incremental run."""
        try:
            metadata = {
                'last_sync_timestamp': timestamp.isoformat(),
                'last_sync_status': 'success'
            }
            if metadata_extra:
                metadata.update(metadata_extra)
            
            blob = self.bucket.blob(metadata_blob_name)
            blob.upload_from_string(json.dumps(metadata))
            logger.info(f"Updated sync timestamp to {timestamp}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating sync timestamp: {e}")
            return False


def _get_feature_server_override(request):
    since_date = None
    since_datetime = None

    try:
        if hasattr(request, 'args') and request.args is not None:
            since_date = request.args.get('since_date')
            since_datetime = request.args.get('since_datetime')
    except Exception as e:
        logger.warning(f"Failed to parse override args (since_date/since_datetime): {e}")

    try:
        body = request.get_json(silent=True) if request is not None else None
        if isinstance(body, dict):
            since_date = since_date or body.get('since_date')
            since_datetime = since_datetime or body.get('since_datetime')
    except Exception as e:
        logger.warning(f"Failed to parse override body (since_date/since_datetime): {e}")

    # Finding #6: Validate since_datetime_override properly to prevent downstream crashes
    if since_datetime:
        try:
            # Test parse it to ensure it's valid ISO format
            datetime.fromisoformat(str(since_datetime))
        except ValueError as e:
            logger.warning(f"Invalid since_datetime override '{since_datetime}', discarded: {e}")
            since_datetime = None

    return since_date, since_datetime


def _get_db_latest_chemical_date(db_path: str) -> str:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(collection_date) FROM chemical_collection_events")
        result = cursor.fetchone()
        conn.close()
        if result and result[0]:
            return str(result[0])
        return '2020-01-01'
    except Exception as e:
        logger.warning(f"Failed to read latest chemical date from DB: {e}")
        return '2020-01-01'


def _run_feature_server_sync(db_manager: 'DatabaseManager', start_time: datetime, request=None):
    logger.info("Running FeatureServer sync mode")

    feature_server_metadata_blob = 'sync_metadata/last_feature_server_sync.json'

    temp_db_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_db:
            temp_db_path = temp_db.name
            if not db_manager.download_database(temp_db.name):
                raise Exception("Failed to download database")

            from data_processing import arcgis_sync
            from chemical_processor import (
                classify_active_sites_in_db,
                insert_processed_data_to_db,
            )
            from site_manager import promote_approved_sites, get_pending_site_summary

            # Ensure pending_sites table exists (DB may predate this feature)
            promote_conn = sqlite3.connect(temp_db.name)
            try:
                promote_conn.execute('''
                    CREATE TABLE IF NOT EXISTS pending_sites (
                        pending_site_id INTEGER PRIMARY KEY,
                        site_name TEXT NOT NULL,
                        latitude REAL,
                        longitude REAL,
                        first_seen_date TEXT NOT NULL,
                        source TEXT DEFAULT 'feature_server',
                        status TEXT DEFAULT 'pending',
                        reviewed_date TEXT,
                        notes TEXT,
                        nearest_site_name TEXT,
                        nearest_site_distance_m REAL,
                        UNIQUE(site_name)
                    )
                ''')
                promote_result = promote_approved_sites(promote_conn)
                if promote_result['promoted'] > 0:
                    logger.info(
                        f"Promoted {promote_result['promoted']} approved sites: "
                        f"{promote_result['names']}"
                    )
            finally:
                promote_conn.close()

            since_date_override, since_datetime_override = _get_feature_server_override(request)
            if since_date_override or since_datetime_override:
                if since_datetime_override:
                    try:
                        last_sync = datetime.fromisoformat(str(since_datetime_override))
                    except Exception:
                        last_sync = since_datetime_override
                    sync_strategy = 'editdate_override'
                    sync_marker = str(since_datetime_override)
                    logger.info(f"FeatureServer sync strategy=editdate_override since={sync_marker}")
                    records = arcgis_sync.fetch_features_edited_since(last_sync)
                else:
                    sync_strategy = 'day_override'
                    sync_marker = str(since_date_override)
                    logger.info(f"FeatureServer sync strategy=day_override since_date={sync_marker}")
                    records = arcgis_sync.fetch_features_since(sync_marker)
            else:
                feature_server_blob = db_manager.bucket.blob(feature_server_metadata_blob)
                if feature_server_blob.exists():
                    existing_metadata = None
                    try:
                        raw_metadata = feature_server_blob.download_as_string()
                        if isinstance(raw_metadata, (bytes, bytearray)):
                            raw_metadata = raw_metadata.decode('utf-8')
                        if isinstance(raw_metadata, str) and raw_metadata.strip():
                            existing_metadata = json.loads(raw_metadata)
                        elif isinstance(raw_metadata, dict):
                            existing_metadata = raw_metadata
                    except Exception as e:
                        logger.warning(
                            f"Unable to parse FeatureServer sync metadata {feature_server_metadata_blob}: {e}"
                        )
                        existing_metadata = None

                    backfill_since_date = None
                    if isinstance(existing_metadata, dict) and existing_metadata.get('needs_backfill'):
                        backfill_since_date = existing_metadata.get('backfill_since_date')

                    if backfill_since_date:
                        sync_strategy = 'day_backfill'
                        sync_marker = str(backfill_since_date)
                        logger.info(
                            f"FeatureServer sync strategy=day_backfill since_date={sync_marker}"
                        )
                        records = arcgis_sync.fetch_features_since(sync_marker)
                    else:
                        last_sync = db_manager.get_last_sync_timestamp(feature_server_metadata_blob)
                        sync_strategy = 'editdate'
                        sync_marker = last_sync.isoformat()
                        logger.info(f"FeatureServer sync strategy=editdate since={sync_marker}")
                        records = arcgis_sync.fetch_features_edited_since(last_sync)
                else:
                    since_date = _get_db_latest_chemical_date(temp_db.name)
                    sync_strategy = 'day'
                    sync_marker = since_date
                    logger.info(f"FeatureServer sync strategy=day since_date={since_date}")
                    records = arcgis_sync.fetch_features_since(since_date)

            if not records:
                logger.info("No new QAQC-complete FeatureServer records found")
                db_manager.update_sync_timestamp(
                    start_time,
                    metadata_blob_name=feature_server_metadata_blob,
                    metadata_extra={
                        'mode': 'feature_server',
                        'sync_strategy': sync_strategy,
                        'sync_marker': sync_marker,
                        'records_fetched': 0,
                    },
                )
                return {
                    'status': 'success',
                    'mode': 'feature_server',
                    'message': 'No new data to process',
                    'records_fetched': 0,
                    'records_processed': 0,
                    'execution_time': str(datetime.now() - start_time),
                    'sync_strategy': sync_strategy,
                    'sync_marker': sync_marker,
                    'current_sync': start_time.isoformat(),
                }

            df = arcgis_sync.translate_to_pipeline_schema(records)
            processed_data = arcgis_sync.process_fetched_data(df)

            if processed_data.empty:
                logger.info("No FeatureServer records produced valid processed data")
                db_manager.update_sync_timestamp(
                    start_time,
                    metadata_blob_name=feature_server_metadata_blob,
                    metadata_extra={
                        'mode': 'feature_server',
                        'sync_strategy': sync_strategy,
                        'sync_marker': sync_marker,
                        'records_fetched': len(records),
                        'records_processed': 0,
                        'records_inserted': 0,
                    },
                )
                return {
                    'status': 'success',
                    'mode': 'feature_server',
                    'message': 'No valid data to insert after processing',
                    'records_fetched': len(records),
                    'records_processed': 0,
                    'records_inserted': 0,
                    'execution_time': str(datetime.now() - start_time),
                    'sync_strategy': sync_strategy,
                    'sync_marker': sync_marker,
                    'current_sync': start_time.isoformat(),
                }

            insert_result = insert_processed_data_to_db(processed_data, temp_db.name)
            if 'error' in insert_result:
                raise Exception(f"Database insertion failed: {insert_result['error']}")

            classification_result = classify_active_sites_in_db(temp_db.name)
            if 'error' in classification_result:
                logger.warning(f"Site classification failed: {classification_result['error']}")
            else:
                logger.info(
                    f"Site classification updated: {classification_result['active_count']} active, "
                    f"{classification_result['historic_count']} historic"
                )

            # Query pending site summary before upload (DB gets deleted after)
            pending_conn = sqlite3.connect(temp_db.name)
            try:
                pending_summary = get_pending_site_summary(pending_conn)
            finally:
                pending_conn.close()

            if not db_manager.upload_database(temp_db.name):
                raise Exception("Failed to upload updated database")
    finally:
        if temp_db_path:
            try:
                os.unlink(temp_db_path)
            except FileNotFoundError:
                pass
            except Exception as e:
                logger.warning(f"Failed to cleanup temp DB file {temp_db_path}: {e}")

    needs_backfill = bool(insert_result.get('new_pending'))
    backfill_since_date = None
    if sync_strategy in ('day', 'day_override'):
        backfill_since_date = sync_marker if needs_backfill else None

    watermark_timestamp = start_time
    if needs_backfill and sync_strategy in ('editdate', 'editdate_override'):
        try:
            watermark_timestamp = last_sync
        except Exception:
            watermark_timestamp = start_time

    db_manager.update_sync_timestamp(
        watermark_timestamp,
        metadata_blob_name=feature_server_metadata_blob,
        metadata_extra={
            'mode': 'feature_server',
            'sync_strategy': sync_strategy,
            'sync_marker': sync_marker,
            'records_fetched': len(records),
            'records_processed': len(processed_data),
            'records_inserted': insert_result.get('records_inserted', 0),
            'pending_sites_promoted': promote_result.get('promoted', 0),
            'new_pending_sites': len(insert_result.get('new_pending', [])),
            'needs_backfill': needs_backfill,
            'backfill_since_date': backfill_since_date,
        },
    )

    result = {
        'status': 'success',
        'mode': 'feature_server',
        'message': f"Successfully processed {len(processed_data)} new records",
        'records_fetched': len(records),
        'records_processed': len(processed_data),
        'records_inserted': insert_result.get('records_inserted', 0),
        'needs_backfill': needs_backfill,
        'backfill_since_date': backfill_since_date,
        'execution_time': str(datetime.now() - start_time),
        'sync_strategy': sync_strategy,
        'sync_marker': sync_marker,
        'current_sync': start_time.isoformat(),
    }

    if 'error' not in classification_result:
        result['site_classification'] = {
            'sites_classified': classification_result.get('sites_classified', 0),
            'active_count': classification_result.get('active_count', 0),
            'historic_count': classification_result.get('historic_count', 0)
        }

    # Add pending sites info to response
    result['pending_sites'] = {
        'new_pending': len(insert_result.get('new_pending', [])),
        'total_pending': pending_summary['total_pending'],
        'promoted': promote_result.get('promoted', 0),
        'coordinate_matched': insert_result.get('coordinate_matched', 0),
        'names': insert_result.get('new_pending', []),
    }

    logger.info(f"FeatureServer sync completed successfully: {result}")
    return result


def survey123_daily_sync(request):
    """
    Cloud Function entry point for daily FeatureServer data sync.

    Fetches new chemical data from the public ArcGIS FeatureServer and
    updates the SQLite database in Cloud Storage.

    NOTE: Entry point name is legacy — retained for GCP config compatibility.
    TODO: Rename to 'data_sync' and update GCP function config.
    """
    start_time = datetime.now()
    logger.info(f"Starting FeatureServer data sync at {start_time}")

    try:
        db_manager = DatabaseManager(DATABASE_BUCKET)
        return _run_feature_server_sync(db_manager, start_time, request)

    except Exception as e:
        error_msg = f"Sync failed: {str(e)}"
        logger.error(error_msg)
        return {
            'status': 'failed',
            'error': error_msg,
            'execution_time': str(datetime.now() - start_time)
        }, 500


if __name__ == "__main__":
    # Local testing support
    class MockRequest:
        pass
    
    result = survey123_daily_sync(MockRequest())
    print(json.dumps(result, indent=2))
