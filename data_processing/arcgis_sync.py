"""
Real-time synchronization of chemical data from the ArcGIS Feature Server.

Fetches new submissions from the public Blue Thumb Feature Server view,
translates field names to match the existing processing pipeline, normalizes
site names, applies QAQC gating, and inserts processed data into the
dashboard database using the shared chemical insertion utility.

The Feature Server endpoint is public (no authentication required) and was
verified against ground-truth records on 2026-01-27.

Field name translation: ArcGIS Feature Server returns internal field names
(e.g. 'SiteName', 'day', 'pH1') while the processing pipeline expects
CSV-export column names (e.g. 'Site Name', 'Sampling Date', 'pH #1').
The Feature Server schema's 'alias' property for each field exactly matches
the CSV column name, so the translation is: rename field → alias.
"""

import os
import re
import sys
from datetime import datetime, timezone

import pandas as pd
import requests

try:
    from data_processing import setup_logging
except ModuleNotFoundError:
    _project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)
    from data_processing import setup_logging
from data_processing.chemical_utils import (
    apply_bdl_conversions,
    insert_chemical_data,
    remove_empty_chemical_rows,
    validate_chemical_data,
)
from data_processing.updated_chemical_processing import (
    format_to_database_schema,
    parse_sampling_dates,
    process_conditional_nutrient,
    process_simple_nutrients,
)
from database.database import close_connection, get_connection

logger = setup_logging("arcgis_sync", category="processing")

# Public Feature Server endpoint (no authentication required).
# Verified working 2026-01-27 against ground-truth records.
FEATURE_SERVER_URL = (
    "https://services5.arcgis.com/L6JGkSUcgPo1zSDi/arcgis/rest/services/"
    "bluethumb_oct2020_view/FeatureServer/0/query"
)

# Maps Feature Server internal field names to the CSV column names that the
# existing processing pipeline expects. Every mapping was verified by matching
# the FS field's 'alias' property against the pipeline's expected column name.
# See data/arcgis_feature_server_schema.json for the full 171-field schema.
ARCGIS_FIELD_MAP = {
    # Identity and date
    'objectid':                          'sample_id',
    'SiteName':                          'Site Name',
    'day':                               'Sampling Date',
    # Core parameters
    'oxygen_sat':                        '% Oxygen Saturation',
    'pH1':                               'pH #1',
    'pH2':                               'pH #2',
    'nitratetest1':                      'Nitrate #1',
    'nitratetest2':                      'Nitrate #2',
    'nitritetest1':                      'Nitrite #1',
    'nitritetest2':                      'Nitrite #2',
    # Ammonia (range-conditional)
    'Ammonia_Range':                     'Ammonia Nitrogen Range Selection',
    'ammonia_Nitrogen2':                 'Ammonia Nitrogen Low Reading #1',
    'ammonia_Nitrogen3':                 'Ammonia Nitrogen Low Reading #2',
    'Ammonia_nitrogen_midrange1_Final':  'Ammonia_nitrogen_midrange1_Final',
    'Ammonia_nitrogen_midrange2_Final':  'Ammonia_nitrogen_midrange2_Final',
    # Orthophosphate (range-conditional)
    'Ortho_Range':                       'Orthophosphate Range Selection',
    'Orthophosphate_Low1_Final':         'Orthophosphate_Low1_Final',
    'Orthophosphate_Low2_Final':         'Orthophosphate_Low2_Final',
    'Orthophosphate_Mid1_Final':         'Orthophosphate_Mid1_Final',
    'Orthophosphate_Mid2_Final':         'Orthophosphate_Mid2_Final',
    'Orthophosphate_High1_Final':        'Orthophosphate_High1_Final',
    'Orthophosphate_High2_Final':        'Orthophosphate_High2_Final',
    # Chloride (range-conditional)
    'Chloride_Range':                    'Chloride Range Selection',
    'Chloride_Low1_Final':               'Chloride_Low1_Final',
    'Chloride_Low2_Final':               'Chloride_Low2_Final',
    'Chloride_High1_Final':              'Chloride_High1_Final',
    'Chloride_High2_Final':              'Chloride_High2_Final',
    # Quality control
    'QAQC_Complete':                     'QAQC_Complete',
}

OUT_FIELDS = list(ARCGIS_FIELD_MAP.keys())


def _normalize_site_name(name):
    """Collapse all runs of whitespace to a single space and strip edges."""
    if pd.isna(name) or name is None:
        return None
    return re.sub(r'\s+', ' ', str(name).strip())


def fetch_features_since(since_date, timeout_seconds=30):
    """
    Fetch QAQC-verified chemical records from the Feature Server since a date.

    Args:
        since_date: 'YYYY-MM-DD' string or datetime object.
        timeout_seconds: HTTP request timeout.

    Returns:
        List of feature attribute dictionaries.
    """
    if isinstance(since_date, datetime):
        since_str = since_date.strftime('%Y-%m-%d')
    else:
        since_str = str(since_date)

    where = (
        f"day >= timestamp '{since_str} 00:00:00' "
        f"AND QAQC_Complete IS NOT NULL"
    )

    return _fetch_features_paginated(
        where=where,
        out_fields=OUT_FIELDS,
        order_by_fields='day DESC',
        timeout_seconds=timeout_seconds,
    )


def fetch_features_edited_since(since_datetime, timeout_seconds=30):
    since_ts_str = None
    if isinstance(since_datetime, datetime):
        dt = since_datetime
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        since_epoch_ms = int(dt.timestamp() * 1000)
        since_ts_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        since_epoch_ms = int(since_datetime)

    out_fields = list(dict.fromkeys(OUT_FIELDS + ['EditDate']))
    where_epoch = f"EditDate >= {since_epoch_ms} AND QAQC_Complete IS NOT NULL"

    try:
        return _fetch_features_paginated(
            where=where_epoch,
            out_fields=out_fields,
            order_by_fields='EditDate DESC',
            timeout_seconds=timeout_seconds,
        )
    except ValueError as e:
        if since_ts_str is None:
            raise
        if 'Invalid query parameters' not in str(e):
            raise

        where_ts = (
            f"EditDate >= timestamp '{since_ts_str}' "
            f"AND QAQC_Complete IS NOT NULL"
        )
        return _fetch_features_paginated(
            where=where_ts,
            out_fields=out_fields,
            order_by_fields='EditDate DESC',
            timeout_seconds=timeout_seconds,
        )


def _fetch_features_paginated(where, out_fields, order_by_fields, timeout_seconds=30):
    result_offset = 0
    page_size = 2000
    records = []

    while True:
        params = {
            'where': where,
            'outFields': ','.join(out_fields),
            'f': 'json',
            'orderByFields': order_by_fields,
            'resultRecordCount': page_size,
            'resultOffset': result_offset,
        }

        logger.info(
            f"Querying Feature Server: offset={result_offset} page_size={page_size} where={where}"
        )

        response = requests.get(
            FEATURE_SERVER_URL, params=params, timeout=timeout_seconds
        )
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict) and 'error' in data:
            raise ValueError(f"ArcGIS error: {data['error']}")

        features = data.get('features', [])
        exceeded = bool(data.get('exceededTransferLimit'))
        logger.info(
            f"Retrieved {len(features)} features from Feature Server (exceeded={exceeded})"
        )

        for f in features:
            attrs = f.get('attributes') if isinstance(f, dict) else None
            if isinstance(attrs, dict):
                records.append(attrs)

        if not features:
            break

        if not exceeded and len(features) < page_size:
            break

        result_offset += len(features)

    return records


def translate_to_pipeline_schema(records):
    """
    Convert raw Feature Server records to a DataFrame with the column names
    the existing chemical processing pipeline expects.

    Handles three transformations:
    1. Rename FS field names to their aliases (= CSV column names)
    2. Convert 'day' epoch-ms to the 'Sampling Date' string format
    3. Normalize site names (collapse double-space after colons, etc.)

    Args:
        records: List of attribute dicts from fetch_features_since().

    Returns:
        DataFrame ready for the chemical processing pipeline.
    """
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Step 1: Rename FS field names → pipeline-expected CSV column names
    rename_map = {k: v for k, v in ARCGIS_FIELD_MAP.items()
                  if k in df.columns and k != v}
    df = df.rename(columns=rename_map)

    # Step 2: Convert 'Sampling Date' from epoch ms to the exact string format
    # that parse_sampling_dates() expects: '%m/%d/%Y, %I:%M %p'
    # Use Central time to match the original CSV timezone.
    if 'Sampling Date' in df.columns:
        dt_utc = pd.to_datetime(df['Sampling Date'], unit='ms', utc=True)
        try:
            df['Sampling Date'] = (
                dt_utc.dt.tz_convert('America/Chicago')
                .dt.strftime('%m/%d/%Y, %I:%M %p')
            )
        except Exception as e:
            logger.warning(f"Failed tz conversion for Sampling Date; falling back to UTC: {e}")
            df['Sampling Date'] = dt_utc.dt.strftime('%m/%d/%Y, %I:%M %p')

    # Step 3: Normalize site names (collapse whitespace)
    if 'Site Name' in df.columns:
        original_names = df['Site Name'].copy()
        df['Site Name'] = df['Site Name'].apply(_normalize_site_name)
        changed = (original_names != df['Site Name']).sum()
        if changed > 0:
            logger.info(f"Normalized {changed} site names (whitespace)")

    # Step 4: Filter to only QAQC-complete records (defense-in-depth;
    # the WHERE clause should already exclude these)
    if 'QAQC_Complete' in df.columns:
        before = len(df)
        df = df[df['QAQC_Complete'].notna()].copy()
        filtered = before - len(df)
        if filtered > 0:
            logger.warning(f"Filtered {filtered} records missing QAQC_Complete")

    logger.info(f"Translated {len(df)} records to pipeline schema")
    return df


def process_fetched_data(df):
    """
    Run translated DataFrame through the existing chemical processing pipeline.

    This is the same sequence used by process_updated_chemical_data(),
    ensuring consistent results.

    Args:
        df: DataFrame with CSV-compatible column names.

    Returns:
        Processed DataFrame ready for database insertion.
    """
    if df.empty:
        return pd.DataFrame()

    logger.info(f"Processing {len(df)} records through chemical pipeline...")

    df = parse_sampling_dates(df)
    df = process_simple_nutrients(df)
    df['Ammonia'] = process_conditional_nutrient(df, 'ammonia')
    df['Orthophosphate'] = process_conditional_nutrient(df, 'orthophosphate')
    df['Chloride'] = process_conditional_nutrient(df, 'chloride')

    formatted_df = format_to_database_schema(df)
    formatted_df = remove_empty_chemical_rows(formatted_df)
    formatted_df = validate_chemical_data(formatted_df, remove_invalid=True)
    formatted_df = apply_bdl_conversions(formatted_df)

    logger.info(f"Processing complete: {len(formatted_df)} rows ready for insertion")
    return formatted_df


def filter_known_sites(df):
    """
    Remove rows whose Site_Name doesn't match any site in the database.

    New sites that appear in the Feature Server but haven't been added to the
    master site list will be logged as warnings and skipped. Run the site
    consolidation pipeline to add them.

    Args:
        df: Processed DataFrame with 'Site_Name' column.

    Returns:
        Tuple of (filtered_df, list_of_skipped_site_names).
    """
    if df.empty or 'Site_Name' not in df.columns:
        return df, []

    from data_processing.data_loader import get_site_lookup_dict

    site_lookup = get_site_lookup_dict()
    known_mask = df['Site_Name'].isin(site_lookup.keys())

    skipped = sorted(df[~known_mask]['Site_Name'].unique().tolist())
    if skipped:
        logger.warning(
            f"Skipping {len(skipped)} unknown site(s) not in database: {skipped}"
        )

    return df[known_mask].copy(), skipped


def get_db_latest_chemical_date():
    """Get the most recent chemical collection date in the database."""
    conn = get_connection()
    try:
        result = conn.execute(
            "SELECT MAX(collection_date) FROM chemical_collection_events"
        ).fetchone()
        if result and result[0]:
            return result[0]
        return '2020-01-01'
    finally:
        close_connection(conn)


def sync_new_chemical_data(since_date=None, dry_run=False):
    """
    End-to-end sync: fetch new data from the Feature Server and insert into DB.

    Pipeline: fetch → translate fields → normalize names → QAQC gate →
    chemical processing → filter known sites → insert.

    Args:
        since_date: 'YYYY-MM-DD' to fetch from. Defaults to the DB's latest date.
        dry_run: If True, fetch and process but skip database insertion.

    Returns:
        Dictionary with sync results and statistics.
    """
    start_time = datetime.now()

    if since_date is None:
        since_date = get_db_latest_chemical_date()

    logger.info(f"=== ArcGIS Sync: fetching records since {since_date} ===")

    # Step 1: Fetch from Feature Server
    records = fetch_features_since(since_date)
    if not records:
        logger.info("No new records found on Feature Server")
        return {
            'status': 'success',
            'records_fetched': 0,
            'records_inserted': 0,
            'since_date': since_date,
            'execution_time': str(datetime.now() - start_time),
        }

    # Step 2: Translate field names and normalize
    df = translate_to_pipeline_schema(records)
    if df.empty:
        return {
            'status': 'success',
            'records_fetched': len(records),
            'records_after_qaqc': 0,
            'records_inserted': 0,
            'since_date': since_date,
            'execution_time': str(datetime.now() - start_time),
        }

    # Step 3: Process through chemical pipeline
    processed_df = process_fetched_data(df)
    if processed_df.empty:
        return {
            'status': 'success',
            'records_fetched': len(records),
            'records_after_processing': 0,
            'records_inserted': 0,
            'since_date': since_date,
            'execution_time': str(datetime.now() - start_time),
        }

    # Step 4: Filter to known sites
    filtered_df, skipped_sites = filter_known_sites(processed_df)

    if dry_run:
        logger.info(f"DRY RUN: would insert {len(filtered_df)} records")
        sample = []
        if not filtered_df.empty:
            sample_df = filtered_df.head(5).copy()
            sample_df['Date'] = sample_df['Date'].astype(str)
            sample = sample_df.to_dict('records')
        return {
            'status': 'dry_run',
            'records_fetched': len(records),
            'records_after_processing': len(processed_df),
            'records_ready': len(filtered_df),
            'skipped_sites': skipped_sites,
            'since_date': since_date,
            'execution_time': str(datetime.now() - start_time),
            'sample_records': sample,
        }

    # Step 5: Insert into database
    if filtered_df.empty:
        return {
            'status': 'success',
            'records_fetched': len(records),
            'records_inserted': 0,
            'skipped_sites': skipped_sites,
            'since_date': since_date,
            'execution_time': str(datetime.now() - start_time),
        }

    stats = insert_chemical_data(filtered_df, data_source="arcgis_feature_server")

    result = {
        'status': 'success',
        'records_fetched': len(records),
        'records_after_processing': len(processed_df),
        'records_inserted': stats.get('measurements_added', 0),
        'events_added': stats.get('events_added', 0),
        'sites_processed': stats.get('sites_processed', 0),
        'skipped_sites': skipped_sites,
        'since_date': since_date,
        'execution_time': str(datetime.now() - start_time),
    }

    logger.info(f"=== Sync complete: {result['records_inserted']} measurements inserted ===")
    return result


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="Sync chemical data from ArcGIS Feature Server to dashboard DB"
    )
    parser.add_argument(
        "--since", default=None,
        help="Fetch records since this date (YYYY-MM-DD). Default: DB's latest date."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch and process without inserting into the database."
    )
    args = parser.parse_args()

    result = sync_new_chemical_data(since_date=args.since, dry_run=args.dry_run)
    print(json.dumps(result, indent=2, default=str))
