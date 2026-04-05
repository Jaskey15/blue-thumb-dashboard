"""
API-first chemical data pipeline for the ArcGIS Feature Server.

Fetches chemical submissions from the public Blue Thumb Feature Server view,
processes them using API field names directly (no CSV translation), normalizes
site names, applies QAQC gating, and inserts processed data into the
dashboard database using the shared chemical insertion utility.

The Feature Server endpoint is public (no authentication required) and was
verified against ground-truth records on 2026-01-27.
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
    SITE_ALIASES,
    apply_bdl_conversions,
    calculate_soluble_nitrogen,
    insert_chemical_data,
    normalize_site_name,
    remove_empty_chemical_rows,
    validate_chemical_data,
)
from data_processing.merge_sites import haversine_m
from database.database import close_connection, get_connection

logger = setup_logging("arcgis_sync", category="processing")

# Public Feature Server endpoint (no authentication required).
# Verified working 2026-01-27 against ground-truth records.
DISTANCE_THRESHOLD_M = 50.0

FEATURE_SERVER_URL = (
    "https://services5.arcgis.com/L6JGkSUcgPo1zSDi/arcgis/rest/services/"
    "bluethumb_oct2020_view/FeatureServer/0/query"
)

# Maps API field names directly to DB column names (only fields that need renaming).
COLUMN_TO_DB = {
    'SiteName': 'Site_Name',
    'oxygen_sat': 'do_percent',
    'Orthophosphate': 'Phosphorus',
}

# Fields to request from Feature Server for chemical processing.
CHEMICAL_FIELDS = [
    'objectid', 'SiteName', 'day', 'oxygen_sat',
    'pH1', 'pH2', 'nitratetest1', 'nitratetest2',
    'nitritetest1', 'nitritetest2',
    'Ammonia_Range', 'ammonia_Nitrogen2', 'ammonia_Nitrogen3',
    'Ammonia_nitrogen_midrange1_Final', 'Ammonia_nitrogen_midrange2_Final',
    'Ortho_Range', 'Orthophosphate_Low1_Final', 'Orthophosphate_Low2_Final',
    'Orthophosphate_Mid1_Final', 'Orthophosphate_Mid2_Final',
    'Orthophosphate_High1_Final', 'Orthophosphate_High2_Final',
    'Chloride_Range', 'Chloride_Low1_Final', 'Chloride_Low2_Final',
    'Chloride_High1_Final', 'Chloride_High2_Final',
    'QAQC_Complete',
]

# Nutrient column mappings using API field names directly.
NUTRIENT_COLUMN_MAPPINGS = {
    'ammonia': {
        'range_selection': 'Ammonia_Range',
        'low_col1': 'ammonia_Nitrogen2',
        'low_col2': 'ammonia_Nitrogen3',
        'mid_col1': 'Ammonia_nitrogen_midrange1_Final',
        'mid_col2': 'Ammonia_nitrogen_midrange2_Final',
    },
    'orthophosphate': {
        'range_selection': 'Ortho_Range',
        'low_col1': 'Orthophosphate_Low1_Final',
        'low_col2': 'Orthophosphate_Low2_Final',
        'mid_col1': 'Orthophosphate_Mid1_Final',
        'mid_col2': 'Orthophosphate_Mid2_Final',
        'high_col1': 'Orthophosphate_High1_Final',
        'high_col2': 'Orthophosphate_High2_Final',
    },
    'chloride': {
        'range_selection': 'Chloride_Range',
        'low_col1': 'Chloride_Low1_Final',
        'low_col2': 'Chloride_Low2_Final',
        'high_col1': 'Chloride_High1_Final',
        'high_col2': 'Chloride_High2_Final',
    },
}

OUT_FIELDS = CHEMICAL_FIELDS


def _normalize_site_name(name):
    # Left as a shim for local logic but now just calls the shared util
    return normalize_site_name(name) or None


def get_greater_value(row, col1, col2, tiebreaker='col1'):
    """Select the greater of two numeric values from a row."""
    try:
        val1 = pd.to_numeric(row[col1], errors='coerce') if pd.notna(row[col1]) else None
        val2 = pd.to_numeric(row[col2], errors='coerce') if pd.notna(row[col2]) else None

        if val1 is None and val2 is None:
            return None
        if val1 is None:
            return val2
        if val2 is None:
            return val1

        if val1 > val2:
            return val1
        elif val2 > val1:
            return val2
        else:
            return val1 if tiebreaker == 'col1' else val2

    except Exception as e:
        logger.warning(f"Error comparing {col1} and {col2}: {e}")
        return None


def get_ph_worst_case(row):
    """Select the pH value furthest from neutral (7)."""
    try:
        ph1 = pd.to_numeric(row['pH1'], errors='coerce')
        ph2 = pd.to_numeric(row['pH2'], errors='coerce')

        if pd.isna(ph1) and pd.isna(ph2):
            return None
        if pd.isna(ph1):
            return ph2
        if pd.isna(ph2):
            return ph1

        dist1 = abs(ph1 - 7)
        dist2 = abs(ph2 - 7)

        if dist2 > dist1:
            return ph2
        else:
            return ph1

    except Exception as e:
        logger.warning(f"Error processing pH values: {e}")
        return None


def get_conditional_nutrient_value(row, range_selection_col, low_col1, low_col2,
                                   mid_col1=None, mid_col2=None,
                                   high_col1=None, high_col2=None):
    """Select a nutrient value based on the specified measurement range."""
    try:
        range_selection = row[range_selection_col]

        if pd.isna(range_selection) or range_selection == '':
            return None

        range_selection = str(range_selection).strip()

        if 'Low' in range_selection:
            return get_greater_value(row, low_col1, low_col2, tiebreaker='col1')
        elif 'Mid' in range_selection and mid_col1 and mid_col2:
            return get_greater_value(row, mid_col1, mid_col2, tiebreaker='col1')
        elif 'High' in range_selection and high_col1 and high_col2:
            return get_greater_value(row, high_col1, high_col2, tiebreaker='col1')
        else:
            logger.warning(f"Unknown range selection: {range_selection}")
            return None

    except Exception as e:
        logger.warning(f"Error processing conditional nutrient value: {e}")
        return None


def process_conditional_nutrient(df, nutrient_name):
    """Apply conditional nutrient logic using NUTRIENT_COLUMN_MAPPINGS."""
    try:
        mapping = NUTRIENT_COLUMN_MAPPINGS[nutrient_name]

        result = df.apply(lambda row: get_conditional_nutrient_value(
            row,
            range_selection_col=mapping['range_selection'],
            low_col1=mapping['low_col1'],
            low_col2=mapping['low_col2'],
            mid_col1=mapping.get('mid_col1'),
            mid_col2=mapping.get('mid_col2'),
            high_col1=mapping.get('high_col1'),
            high_col2=mapping.get('high_col2'),
        ), axis=1)

        logger.info(f"Successfully processed {nutrient_name} values")
        return result

    except Exception as e:
        logger.error(f"Error processing {nutrient_name}: {e}")
        return pd.Series([None] * len(df))


def process_simple_nutrients(df):
    """Process nutrients that require selecting the greater of two values."""
    try:
        df['Nitrate'] = df.apply(
            lambda row: get_greater_value(row, 'nitratetest1', 'nitratetest2'), axis=1
        )
        df['Nitrite'] = df.apply(
            lambda row: get_greater_value(row, 'nitritetest1', 'nitritetest2'), axis=1
        )
        logger.info("Successfully processed Nitrate and Nitrite values")
        return df
    except Exception as e:
        logger.error(f"Error processing simple nutrients: {e}")
        return df


def parse_epoch_dates(df):
    """Convert 'day' epoch ms directly to Date, Year, Month columns using Central timezone."""
    try:
        dt_utc = pd.to_datetime(df['day'], unit='ms', utc=True)
        try:
            dt_central = dt_utc.dt.tz_convert('America/Chicago')
        except Exception as e:
            logger.warning(f"Failed tz conversion; falling back to UTC: {e}")
            dt_central = dt_utc

        df['Date'] = dt_central.dt.normalize().dt.tz_localize(None)
        df['Year'] = dt_central.dt.year
        df['Month'] = dt_central.dt.month

        logger.info(f"Successfully parsed {len(df)} dates")
        if not df['Date'].isna().all():
            logger.info(f"Date range: {df['Date'].min()} to {df['Date'].max()}")

        return df

    except Exception as e:
        logger.error(f"Error parsing epoch dates: {e}")
        return df


def format_to_database_schema(df):
    """Format processed data to match the database schema."""
    try:
        formatted_df = df.copy()
        has_sample_id = 'sample_id' in formatted_df.columns

        formatted_df['pH'] = formatted_df.apply(get_ph_worst_case, axis=1)
        formatted_df = formatted_df.rename(columns=COLUMN_TO_DB)
        formatted_df = calculate_soluble_nitrogen(formatted_df)

        required_columns = [
            'Site_Name', 'Date', 'Year', 'Month', 'do_percent', 'pH',
            'Nitrate', 'Nitrite', 'Ammonia', 'Phosphorus', 'Chloride',
            'soluble_nitrogen',
        ]
        if has_sample_id:
            required_columns.append('sample_id')

        # Preserve geometry columns for Haversine site resolution
        if 'latitude' in formatted_df.columns:
            required_columns.append('latitude')
        if 'longitude' in formatted_df.columns:
            required_columns.append('longitude')

        formatted_df = formatted_df[required_columns]

        numeric_columns = [
            'do_percent', 'pH', 'Nitrate', 'Nitrite', 'Ammonia',
            'Phosphorus', 'Chloride', 'soluble_nitrogen',
        ]
        for col in numeric_columns:
            formatted_df[col] = pd.to_numeric(formatted_df[col], errors='coerce')

        logger.info(f"Successfully formatted {len(formatted_df)} rows to database schema")
        return formatted_df

    except Exception as e:
        logger.error(f"Error formatting data to database schema: {e}")
        return pd.DataFrame()


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
        return_geometry=True,
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
            return_geometry=True,
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
            return_geometry=True,
        )


def _fetch_features_paginated(where, out_fields, order_by_fields, timeout_seconds=30,
                              return_geometry=False):
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
            'returnGeometry': return_geometry,
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
            if return_geometry:
                # Return full feature dicts so geometry is accessible
                if isinstance(f, dict):
                    records.append(f)
            else:
                attrs = f.get('attributes') if isinstance(f, dict) else None
                if isinstance(attrs, dict):
                    records.append(attrs)

        if not features:
            break

        if not exceeded and len(features) < page_size:
            break

        result_offset += len(features)

    return records


def prepare_dataframe(records):
    """
    Convert raw Feature Server records into a flat DataFrame.

    Handles both attribute-only dicts (return_geometry=False) and full
    feature dicts (return_geometry=True). When geometry is present,
    latitude and longitude are extracted into columns for downstream
    Haversine matching.

    Args:
        records: List of dicts from _fetch_features_paginated().

    Returns:
        DataFrame with API field columns, plus latitude/longitude when
        geometry was included. objectid is renamed to sample_id.
    """
    if not records:
        return pd.DataFrame()

    # Detect format: full feature dicts have an 'attributes' key
    if records and isinstance(records[0], dict) and 'attributes' in records[0]:
        rows = []
        for record in records:
            attrs = record.get('attributes', {})
            geom = record.get('geometry') or {}
            attrs['latitude'] = geom.get('y')
            attrs['longitude'] = geom.get('x')
            rows.append(attrs)
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(records)

    if df.empty:
        return df

    # Normalize site names
    if 'SiteName' in df.columns:
        df['SiteName'] = df['SiteName'].apply(_normalize_site_name)

    # Filter to only QAQC-complete records (defense-in-depth)
    if 'QAQC_Complete' in df.columns:
        before = len(df)
        df = df[df['QAQC_Complete'].notna()].copy()
        filtered = before - len(df)
        if filtered > 0:
            logger.warning(f"Filtered {filtered} records missing QAQC_Complete")

    # Rename objectid → sample_id
    if 'objectid' in df.columns:
        df = df.rename(columns={'objectid': 'sample_id'})

    logger.info(f"Prepared {len(df)} records for processing")
    return df


def process_fetched_data(df):
    """
    Process a DataFrame with API field names through the chemical pipeline.

    Args:
        df: DataFrame with API field names from prepare_dataframe().

    Returns:
        Processed DataFrame ready for database insertion.
    """
    if df.empty:
        return pd.DataFrame()

    logger.info(f"Processing {len(df)} records through chemical pipeline...")

    df = parse_epoch_dates(df)
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


def resolve_unknown_sites(df, conn=None):
    """Resolve unknown site names instead of dropping them.

    Walks a resolution chain for each unique Site_Name in the DataFrame:
    1. Exact match — name exists in DB
    2. Normalized match — normalize + casefold matches a DB name
    3. Alias lookup — SITE_ALIASES maps to a canonical DB name
    4. Haversine match — coordinates within 50m of existing site
    5. Auto-insert — create new site in DB

    Args:
        df: DataFrame with 'Site_Name', 'latitude', 'longitude' columns.
        conn: SQLite connection. If None, one is obtained and closed internally.

    Returns:
        Tuple of (resolved_df, stats_dict) where resolved_df has Site_Name
        remapped to DB names, and stats_dict has resolution counts.
    """
    if df.empty or 'Site_Name' not in df.columns:
        return df.copy(), {
            'already_known': 0, 'normalized_match': 0,
            'alias_match': 0, 'coordinate_match': 0, 'auto_inserted': 0,
        }

    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        # Load all sites from DB
        rows = conn.execute(
            "SELECT site_id, site_name, latitude, longitude FROM sites"
        ).fetchall()
        site_lookup = {row[1]: row[0] for row in rows}
        existing_sites = [(row[0], row[1], row[2], row[3]) for row in rows]

        # Build normalized lookup: normalized_casefold -> db_name
        norm_lookup = {}
        for db_name in site_lookup:
            norm_key = normalize_site_name(db_name).casefold()
            norm_lookup[norm_key] = db_name

        stats = {
            'already_known': 0, 'normalized_match': 0,
            'alias_match': 0, 'coordinate_match': 0, 'auto_inserted': 0,
        }
        name_map = {}

        for site_name in df['Site_Name'].unique():
            # 1. Exact match
            if site_name in site_lookup:
                name_map[site_name] = site_name
                stats['already_known'] += 1
                continue

            # 2. Normalized match
            norm_key = normalize_site_name(site_name).casefold()
            if norm_key in norm_lookup:
                db_name = norm_lookup[norm_key]
                name_map[site_name] = db_name
                stats['normalized_match'] += 1
                logger.info(f"Normalized match: '{site_name}' -> '{db_name}'")
                continue

            # 3. Alias lookup
            canonical = SITE_ALIASES.get(norm_key)
            if canonical:
                canon_norm = normalize_site_name(canonical).casefold()
                if canon_norm in norm_lookup:
                    db_name = norm_lookup[canon_norm]
                    name_map[site_name] = db_name
                    stats['alias_match'] += 1
                    logger.info(f"Alias match: '{site_name}' -> '{db_name}'")
                    continue

            # 4. Haversine coordinate match
            site_rows = df[df['Site_Name'] == site_name]
            lat_vals = site_rows['latitude'].dropna()
            lon_vals = site_rows['longitude'].dropna()
            matched_coord = False
            if len(lat_vals) > 0 and len(lon_vals) > 0:
                lat = lat_vals.iloc[0]
                lon = lon_vals.iloc[0]
                for site_id, existing_name, ex_lat, ex_lon in existing_sites:
                    if ex_lat is None or ex_lon is None:
                        continue
                    dist = haversine_m(lat, lon, ex_lat, ex_lon)
                    if dist <= DISTANCE_THRESHOLD_M:
                        name_map[site_name] = existing_name
                        stats['coordinate_match'] += 1
                        logger.info(
                            f"Coordinate match: '{site_name}' is {dist:.1f}m from "
                            f"'{existing_name}'"
                        )
                        matched_coord = True
                        break
            if matched_coord:
                continue

            # 5. Auto-insert
            insert_lat = lat_vals.iloc[0] if len(lat_vals) > 0 else None
            insert_lon = lon_vals.iloc[0] if len(lon_vals) > 0 else None
            conn.execute(
                "INSERT OR IGNORE INTO sites (site_name, latitude, longitude, active) "
                "VALUES (?, ?, ?, 1)",
                (site_name, insert_lat, insert_lon),
            )
            conn.commit()
            new_row = conn.execute(
                "SELECT site_id FROM sites WHERE site_name = ?", (site_name,)
            ).fetchone()
            site_lookup[site_name] = new_row[0]
            existing_sites.append((new_row[0], site_name, insert_lat, insert_lon))
            norm_lookup[normalize_site_name(site_name).casefold()] = site_name
            name_map[site_name] = site_name
            stats['auto_inserted'] += 1
            logger.info(f"Auto-inserted new site: '{site_name}' (site_id={new_row[0]})")

        resolved_df = df.copy()
        resolved_df['Site_Name'] = resolved_df['Site_Name'].map(name_map)
        return resolved_df, stats

    finally:
        if own_conn:
            close_connection(conn)


def fetch_site_data(timeout_seconds=30):
    """
    Fetch distinct site names, coordinates, and county from the Feature Server.

    Used by consolidate_sites.py to register Feature Server sites during
    site consolidation (priority 4 slot).

    Returns:
        DataFrame with columns: site_name, latitude, longitude, county,
        river_basin, ecoregion, source_file, source_description
    """
    records = _fetch_features_paginated(
        where="1=1",
        out_fields=['SiteName', 'CountyName'],
        order_by_fields='SiteName ASC',
        timeout_seconds=timeout_seconds,
        return_geometry=True,
    )

    if not records:
        logger.warning("No sites found on Feature Server")
        return pd.DataFrame()

    rows = []
    for record in records:
        attrs = record.get('attributes', record)
        geom = record.get('geometry', {})
        rows.append({
            'site_name': _normalize_site_name(attrs.get('SiteName')),
            'latitude': geom.get('y'),
            'longitude': geom.get('x'),
            'county': attrs.get('CountyName'),
            'river_basin': None,
            'ecoregion': None,
            'source_file': 'arcgis_feature_server',
            'source_description': 'ArcGIS Feature Server',
        })

    df = pd.DataFrame(rows)
    df = df[df['site_name'].notna() & (df['site_name'] != '')]

    # Deduplicate by site name, warn if coordinates differ
    before = len(df)
    dupes_mask = df.duplicated(subset=['site_name'], keep='first')
    if dupes_mask.any():
        for site_name in df.loc[dupes_mask, 'site_name'].unique():
            site_rows = df[df['site_name'] == site_name]
            lat_diff = site_rows['latitude'].nunique() > 1
            lon_diff = site_rows['longitude'].nunique() > 1
            if lat_diff or lon_diff:
                coords = [(r['latitude'], r['longitude']) for _, r in site_rows.iterrows()]
                logger.warning(f"Site '{site_name}' has differing coordinates: {coords}")
        df = df[~dupes_mask]
        dupes = before - len(df)
        logger.info(f"Deduplicated {dupes} duplicate site entries")

    logger.info(f"Fetched {len(df)} unique sites from Feature Server")
    return df


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

    # Step 2: Prepare DataFrame (normalize names, QAQC filter, rename objectid)
    df = prepare_dataframe(records)
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


def sync_all_chemical_data(dry_run=False):
    """
    Fetch ALL current-period chemical records from Feature Server and insert into DB.

    Used by reset_database.py for full database rebuilds. Unlike sync_new_chemical_data()
    which fetches incrementally by date, this fetches everything.

    Note: Site resolution logic here duplicates cloud_functions/survey123_sync/site_manager.py.
    This local path is intended for one-time DB rebuilds — daily sync is handled by the
    Cloud Function. If this function sees continued use, consider extracting shared
    site resolution into data_processing/site_manager.py.

    Args:
        dry_run: If True, fetch and process but skip database insertion.

    Returns:
        Dictionary with sync results and statistics.
    """
    start_time = datetime.now()
    logger.info("=== ArcGIS Full Sync: fetching ALL records ===")

    records = _fetch_features_paginated(
        where="QAQC_Complete IS NOT NULL",
        out_fields=CHEMICAL_FIELDS,
        order_by_fields='day ASC',
        return_geometry=True,
    )

    if not records:
        logger.info("No records found on Feature Server")
        return {
            'status': 'success',
            'records_fetched': 0,
            'records_inserted': 0,
            'execution_time': str(datetime.now() - start_time),
        }

    df = prepare_dataframe(records)
    if df.empty:
        return {
            'status': 'success',
            'records_fetched': len(records),
            'records_after_qaqc': 0,
            'records_inserted': 0,
            'execution_time': str(datetime.now() - start_time),
        }

    processed_df = process_fetched_data(df)
    if processed_df.empty:
        return {
            'status': 'success',
            'records_fetched': len(records),
            'records_after_processing': 0,
            'records_inserted': 0,
            'execution_time': str(datetime.now() - start_time),
        }

    conn = get_connection()
    try:
        resolved_df, site_stats = resolve_unknown_sites(processed_df, conn)
    finally:
        close_connection(conn)

    if dry_run:
        logger.info(f"DRY RUN: would insert {len(resolved_df)} records")
        return {
            'status': 'dry_run',
            'records_fetched': len(records),
            'records_after_processing': len(processed_df),
            'records_ready': len(resolved_df),
            'site_resolution': site_stats,
            'execution_time': str(datetime.now() - start_time),
        }

    if resolved_df.empty:
        return {
            'status': 'success',
            'records_fetched': len(records),
            'records_inserted': 0,
            'site_resolution': site_stats,
            'execution_time': str(datetime.now() - start_time),
        }

    stats = insert_chemical_data(resolved_df, data_source="arcgis_feature_server")

    result = {
        'status': 'success',
        'records_fetched': len(records),
        'records_after_processing': len(processed_df),
        'records_inserted': stats.get('measurements_added', 0),
        'events_added': stats.get('events_added', 0),
        'sites_processed': stats.get('sites_processed', 0),
        'sites_auto_inserted': site_stats.get('auto_inserted', 0),
        'site_resolution': site_stats,
        'execution_time': str(datetime.now() - start_time),
    }

    logger.info(f"=== Full sync complete: {result['records_inserted']} measurements inserted ===")
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
