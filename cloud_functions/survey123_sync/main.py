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
        try:
            # Create timestamped backup before updating
            backup_name = f"backups/blue_thumb_backup_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')}.db"
            blob = self.bucket.blob(self.db_blob_name)
            if blob.exists():
                backup_blob = self.bucket.blob(backup_name)
                backup_blob.upload_from_string(blob.download_as_string())
                logger.info(f"Created backup: {backup_name}")
            
            new_blob = self.bucket.blob(self.db_blob_name)
            new_blob.upload_from_filename(local_path)
            logger.info(f"Uploaded updated database to {self.db_blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading database: {e}")
            return False
    
    def get_last_sync_timestamp(self, metadata_blob_name: str) -> datetime:
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
        metadata_blob_name: str,
        metadata_extra: dict | None = None,
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


def _run_feature_server_sync(db_manager: 'DatabaseManager', start_time: datetime):
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

            feature_server_blob = db_manager.bucket.blob(feature_server_metadata_blob)
            if feature_server_blob.exists():
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

    db_manager.update_sync_timestamp(
        start_time,
        metadata_blob_name=feature_server_metadata_blob,
        metadata_extra={
            'mode': 'feature_server',
            'sync_strategy': sync_strategy,
            'sync_marker': sync_marker,
            'records_fetched': len(records),
            'records_processed': len(processed_data),
            'records_inserted': insert_result.get('records_inserted', 0),
        },
    )

    result = {
        'status': 'success',
        'mode': 'feature_server',
        'message': f"Successfully processed {len(processed_data)} new records",
        'records_fetched': len(records),
        'records_processed': len(processed_data),
        'records_inserted': insert_result.get('records_inserted', 0),
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

    logger.info(f"FeatureServer sync completed successfully: {result}")
    return result


@functions_framework.http
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
        return _run_feature_server_sync(db_manager, start_time)

    except Exception as e:
        error_msg = f"Sync failed: {str(e)}"
        logger.error(error_msg)
        return {
            'status': 'failed',
            'error': error_msg,
            'execution_time': str(datetime.now() - start_time)
        }, 500


if __name__ == "__main__":
    result = survey123_daily_sync(None)
    print(json.dumps(result, indent=2))