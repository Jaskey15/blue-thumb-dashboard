import os
import sqlite3
import time

from config.gcp_config import is_gcp_environment, get_database_path
from utils import setup_logging

logger = setup_logging("database", category="database")

_GCS_DB_BLOB_NAME = os.environ.get("GCS_DB_BLOB_NAME", "blue_thumb.db")
_DB_REFRESH_INTERVAL_SECONDS = int(os.environ.get("DB_REFRESH_INTERVAL_SECONDS", "300"))

_refresh_thread_started = False
_refresh_lock = None
_last_seen_generation = None
_last_generation_check_monotonic = 0.0


def _should_use_gcs_backed_db() -> bool:
    return bool(os.environ.get("K_SERVICE") or os.environ.get("GAE_APPLICATION"))


def _get_local_db_path():
    return os.path.join(os.path.dirname(__file__), "blue_thumb.db")


def _download_db_from_gcs(destination_path: str) -> bool:
    bucket_name = os.environ.get("GCS_BUCKET_DATABASE")
    if not bucket_name:
        logger.warning("GCS_BUCKET_DATABASE not set; cannot download database from GCS")
        return False

    try:
        from google.cloud import storage

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(_GCS_DB_BLOB_NAME)

        if not blob.exists():
            logger.error(
                f"Database blob '{_GCS_DB_BLOB_NAME}' not found in bucket '{bucket_name}'"
            )
            return False

        tmp_path = f"{destination_path}.tmp"
        blob.download_to_filename(tmp_path)
        os.replace(tmp_path, destination_path)
        logger.info(f"Downloaded database to {destination_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to download database from GCS: {e}")
        return False


def _refresh_loop(destination_path: str):
    global _last_seen_generation

    bucket_name = os.environ.get("GCS_BUCKET_DATABASE")
    if not bucket_name:
        return

    try:
        from google.cloud import storage

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(_GCS_DB_BLOB_NAME)
    except Exception as e:
        logger.error(f"Failed to initialize GCS client for DB refresh: {e}")
        return

    while True:
        try:
            import time

            time.sleep(_DB_REFRESH_INTERVAL_SECONDS)

            blob.reload()
            generation = getattr(blob, "generation", None)
            if generation is not None and generation == _last_seen_generation:
                continue

            if _download_db_from_gcs(destination_path):
                _last_seen_generation = generation
        except Exception as e:
            logger.error(f"Database refresh loop error: {e}")


def _ensure_gcp_db_ready(db_path: str):
    global _refresh_thread_started, _refresh_lock, _last_seen_generation

    if _refresh_lock is None:
        import threading

        _refresh_lock = threading.Lock()

    with _refresh_lock:
        if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
            _download_db_from_gcs(db_path)

        if not _refresh_thread_started:
            bucket_name = os.environ.get("GCS_BUCKET_DATABASE")
            if bucket_name:
                try:
                    from google.cloud import storage
                    import threading

                    client = storage.Client()
                    bucket = client.bucket(bucket_name)
                    blob = bucket.blob(_GCS_DB_BLOB_NAME)
                    if blob.exists():
                        blob.reload()
                        _last_seen_generation = getattr(blob, "generation", None)
                except Exception as e:
                    logger.warning(f"Unable to read initial GCS DB generation: {e}")

                import threading

                t = threading.Thread(target=_refresh_loop, args=(db_path,), daemon=True)
                t.start()
                _refresh_thread_started = True


def _maybe_refresh_gcp_db_on_request(db_path: str):
    global _last_seen_generation, _last_generation_check_monotonic

    bucket_name = os.environ.get("GCS_BUCKET_DATABASE")
    if not bucket_name:
        return

    now = time.monotonic()
    if (
        _last_generation_check_monotonic
        and (now - _last_generation_check_monotonic) < _DB_REFRESH_INTERVAL_SECONDS
    ):
        return

    _last_generation_check_monotonic = now

    try:
        from google.cloud import storage

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(_GCS_DB_BLOB_NAME)

        if not blob.exists():
            return

        blob.reload()
        generation = getattr(blob, "generation", None)
        if generation is None or generation == _last_seen_generation:
            return

        if _refresh_lock is not None:
            with _refresh_lock:
                if _download_db_from_gcs(db_path):
                    _last_seen_generation = generation
        else:
            if _download_db_from_gcs(db_path):
                _last_seen_generation = generation
    except Exception as e:
        logger.warning(f"Unable to refresh DB from GCS on request: {e}")

def get_connection():
    """Create and return a database connection."""
    db_path = _get_local_db_path()

    if is_gcp_environment() and _should_use_gcs_backed_db():
        gcp_db_path = get_database_path()
        _ensure_gcp_db_ready(gcp_db_path)
        _maybe_refresh_gcp_db_on_request(gcp_db_path)

        if os.path.exists(gcp_db_path) and os.path.getsize(gcp_db_path) > 0:
            db_path = gcp_db_path
        else:
            logger.warning(
                "GCP environment detected but /tmp database unavailable; falling back to bundled DB"
            )

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    
    return conn

def close_connection(conn):
    """Safely close a database connection."""
    if conn:
        conn.commit()
        conn.close()

def execute_query(query, params=None):
    """Execute a SQL query with error handling."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        return cursor
    except Exception as e:
        print(f"Database error: {e}")
        conn.rollback()
        raise
    finally:
        close_connection(conn)