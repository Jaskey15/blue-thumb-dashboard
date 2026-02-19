"""
Chemical processing adapter for Survey123 sync Cloud Function.

Reuses existing processing pipeline for consistency with main dashboard.
"""

import logging
import os
import re
import sqlite3
import sys
from typing import Any, Dict

import pandas as pd

_candidate_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir)
)
if (
    os.path.isdir(os.path.join(_candidate_root, 'data_processing'))
    and os.path.isdir(os.path.join(_candidate_root, 'database'))
    and _candidate_root not in sys.path
):
    sys.path.insert(0, _candidate_root)

from data_processing.chemical_utils import (
    apply_bdl_conversions,
    determine_status,
    insert_collection_event,
    remove_empty_chemical_rows,
    validate_chemical_data,
)
from data_processing.updated_chemical_processing import (
    format_to_database_schema,
    parse_sampling_dates,
    process_conditional_nutrient,
    process_simple_nutrients,
)

logger = logging.getLogger(__name__)

def get_reference_values_from_db(conn):
    """
    Load chemical reference values for status determination.
    """
    try:
        reference_values = {}
        
        query = """
        SELECT p.parameter_code, r.threshold_type, r.value
        FROM chemical_reference_values r
        JOIN chemical_parameters p ON r.parameter_id = p.parameter_id
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            raise Exception("No chemical reference values found in database")
        
        for param in df['parameter_code'].unique():
            reference_values[param] = {}
            param_data = df[df['parameter_code'] == param]
            
            # Map database threshold types to dashboard reference keys
            threshold_mapping = {
                'normal_min': 'normal min',
                'normal_max': 'normal max',
                'caution_min': 'caution min',
                'caution_max': 'caution max',
                'normal': 'normal',
                'caution': 'caution',
                'poor': 'poor'
            }

            for _, row in param_data.iterrows():
                if row['threshold_type'] in threshold_mapping:
                    reference_key = threshold_mapping[row['threshold_type']]
                    reference_values[param][reference_key] = row['value']
        
        if not reference_values:
            raise Exception("Failed to parse chemical reference values from database")
            
        logger.debug(f"Retrieved reference values for {len(reference_values)} parameters")
        return reference_values
        
    except Exception as e:
        logger.error(f"Error getting reference values: {e}")
        raise Exception(f"Cannot retrieve chemical reference values from database: {e}")

def process_survey123_chemical_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply complete chemical processing pipeline to Survey123 data.
    
    Uses identical workflow as main processing to ensure consistency.
    """
    try:
        logger.info("Starting complete processing of Survey123 chemical data...")
        
        if df.empty:
            logger.warning("Empty DataFrame provided")
            return pd.DataFrame()
        
        # Processing pipeline steps
        df = parse_sampling_dates(df)
        df = process_simple_nutrients(df)  # Nitrate, Nitrite
        df['Ammonia'] = process_conditional_nutrient(df, 'ammonia')
        df['Orthophosphate'] = process_conditional_nutrient(df, 'orthophosphate') 
        df['Chloride'] = process_conditional_nutrient(df, 'chloride')
        
        # Format and validate
        formatted_df = format_to_database_schema(df)
        formatted_df = remove_empty_chemical_rows(formatted_df)
        formatted_df = validate_chemical_data(formatted_df, remove_invalid=True)
        formatted_df = apply_bdl_conversions(formatted_df)
        
        logger.info(f"Complete processing finished: {len(formatted_df)} rows ready for database")
        return formatted_df
        
    except Exception as e:
        logger.error(f"Error in complete processing pipeline: {e}")
        return pd.DataFrame()

def insert_processed_data_to_db(df: pd.DataFrame, db_path: str) -> Dict[str, Any]:
    """
    Insert processed chemical data with proper site linking and status calculation.
    """
    if df.empty:
        return {'records_inserted': 0, 'error': 'No data to insert'}
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_chemical_collection_events_sample_id
            ON chemical_collection_events(sample_id)
            WHERE sample_id IS NOT NULL
            """
        )
        
        reference_values = get_reference_values_from_db(conn)
        
        # Site lookup for ID mapping
        site_query = "SELECT site_id, site_name FROM sites"
        site_df = pd.read_sql_query(site_query, conn)
        site_lookup = dict(zip(site_df['site_name'], site_df['site_id']))

        def _normalize_site_name(name: Any) -> str:
            if name is None or pd.isna(name):
                return ''
            normalized = re.sub(r'\s+', ' ', str(name).strip())
            return normalized.rstrip('.')

        normalized_site_lookup: Dict[str, int] = {}
        for db_site_name, db_site_id in site_lookup.items():
            normalized = _normalize_site_name(db_site_name).casefold()
            if normalized and normalized not in normalized_site_lookup:
                normalized_site_lookup[normalized] = db_site_id

        site_aliases = {
            'cow creek: virginia avenue': 'Cow Creek: West Virginia Avenue',
            'cow creek: virginia ave': 'Cow Creek: West Virginia Avenue',
            'blue beaver creek: cache rd': 'Blue Beaver Creek: Pecan Road',
            'mooser creek: riverfield': 'Mooser Creek Trib: Riverfield School',
            'deep fork river: canyon park': 'Deep Fork Tributary: Classen',
            'tributary to arkansas river: walton': 'Unknown Trib to Arkansas River',
        }
        
        records_inserted = 0
        skipped_records_unknown_sites = 0
        unknown_sites = set()
        unknown_site_counts: Dict[str, int] = {}
        unknown_site_sample_ids: Dict[str, Any] = {}
        unknown_site_sample_ids_truncated = False
        unknown_site_sample_ids_limit_per_site = 50
        has_sample_id = 'sample_id' in df.columns
        
        for _, row in df.iterrows():
            site_name = row['Site_Name']

            site_id = site_lookup.get(site_name)
            if site_id is None:
                normalized_key = _normalize_site_name(site_name).casefold()
                site_id = normalized_site_lookup.get(normalized_key)

            if site_id is None:
                normalized_key = _normalize_site_name(site_name).casefold()
                canonical_name = site_aliases.get(normalized_key)
                if canonical_name:
                    site_id = site_lookup.get(canonical_name)
                    if site_id is None:
                        site_id = normalized_site_lookup.get(
                            _normalize_site_name(canonical_name).casefold()
                        )
                    if site_id is not None:
                        site_name = canonical_name

            if site_id is None:
                site_name_str = str(site_name).strip()
                unknown_sites.add(site_name_str)
                skipped_records_unknown_sites += 1
                unknown_site_counts[site_name_str] = unknown_site_counts.get(site_name_str, 0) + 1

                if has_sample_id:
                    raw_sample_id = row.get('sample_id')
                    sample_id_int = None
                    if raw_sample_id is not None and pd.notna(raw_sample_id):
                        try:
                            sample_id_int = int(raw_sample_id)
                        except Exception:
                            sample_id_int = None

                    if sample_id_int is not None:
                        existing = unknown_site_sample_ids.get(site_name_str)
                        if existing is None:
                            unknown_site_sample_ids[site_name_str] = [sample_id_int]
                        elif isinstance(existing, list):
                            if len(existing) < unknown_site_sample_ids_limit_per_site:
                                existing.append(sample_id_int)
                            else:
                                unknown_site_sample_ids_truncated = True
                logger.warning(f"Site {site_name} not found in database - skipping")
                continue
            date_str = row['Date'].strftime('%Y-%m-%d')

            sample_id = None
            if has_sample_id:
                sample_id = row.get('sample_id')

            if sample_id is not None and pd.notna(sample_id):
                event_id = insert_collection_event(
                    cursor,
                    site_id=site_id,
                    date_str=date_str,
                    year=row['Year'],
                    month=row['Month'],
                    site_name=site_name,
                    sample_id=sample_id,
                )
            else:
                event_id = insert_collection_event(
                    cursor,
                    site_id=site_id,
                    date_str=date_str,
                    year=row['Year'],
                    month=row['Month'],
                    site_name=site_name,
                    sample_id=None,
                )
            
            # Parameter measurements insertion
            parameter_map = {
                'do_percent': 1, 'pH': 2, 'soluble_nitrogen': 3, 
                'Phosphorus': 4, 'Chloride': 5
            }
            
            for param_name, param_id in parameter_map.items():
                if param_name in row and pd.notna(row[param_name]):
                    value = row[param_name]
                    status = determine_status(param_name, value, reference_values)
                    
                    measurement_query = """
                        INSERT OR REPLACE INTO chemical_measurements 
                        (event_id, parameter_id, value, status)
                        VALUES (?, ?, ?, ?)
                    """
                    cursor.execute(measurement_query, (event_id, param_id, value, status))
                    records_inserted += 1
        
        conn.commit()
        conn.close()
        
        logger.info(f"Successfully inserted {records_inserted} measurements")
        return {
            'records_inserted': records_inserted,
            'skipped_records_unknown_sites': skipped_records_unknown_sites,
            'unknown_sites': sorted(unknown_sites),
            'unknown_site_counts': dict(sorted(unknown_site_counts.items())),
            'unknown_site_sample_ids': dict(sorted(unknown_site_sample_ids.items())),
            'unknown_site_sample_ids_truncated': unknown_site_sample_ids_truncated,
            'unknown_site_sample_ids_limit_per_site': unknown_site_sample_ids_limit_per_site,
        }
        
    except Exception as e:
        logger.error(f"Error inserting data to database: {e}")
        return {'records_inserted': 0, 'error': str(e)}

def classify_active_sites_in_db(db_path: str) -> Dict[str, Any]:
    """
    Classifies sites as active or historic based on recent chemical data.
    
    A site is "active" if it has a chemical reading within one year of the
    most recent reading date across all sites. Otherwise, it is "historic".
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        Dictionary with classification results and counts
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Step 1: Find the most recent chemical reading date across all sites
        cursor.execute("""
            SELECT MAX(collection_date) 
            FROM chemical_collection_events
        """)
        
        result = cursor.fetchone()
        if not result or not result[0]:
            logger.warning("No chemical data found - cannot classify active sites")
            return {'error': 'No chemical data found', 'sites_classified': 0}
            
        most_recent_date = result[0]
        
        # Step 2: Calculate cutoff date (1 year before most recent reading)
        from datetime import datetime, timedelta
        most_recent_dt = datetime.strptime(most_recent_date, '%Y-%m-%d')
        cutoff_date = most_recent_dt - timedelta(days=365)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')
        
        # Step 3: Get the most recent chemical reading date for each site
        cursor.execute("""
            SELECT s.site_id, s.site_name, MAX(c.collection_date) as last_reading
            FROM sites s
            LEFT JOIN chemical_collection_events c ON s.site_id = c.site_id
            GROUP BY s.site_id, s.site_name
        """)
        
        sites_data = cursor.fetchall()
        active_count = 0
        historic_count = 0
        
        # Step 4: Update each site's active status
        for site_id, site_name, last_reading in sites_data:
            if last_reading and last_reading >= cutoff_date_str:
                cursor.execute("""
                    UPDATE sites 
                    SET active = 1, last_chemical_reading_date = ?
                    WHERE site_id = ?
                """, (last_reading, site_id))
                active_count += 1
            else:
                cursor.execute("""
                    UPDATE sites 
                    SET active = 0, last_chemical_reading_date = ?
                    WHERE site_id = ?
                """, (last_reading, site_id))
                historic_count += 1
        
        conn.commit()
        conn.close()
        
        logger.info(f"Site classification complete: {active_count} active, {historic_count} historic")
        
        return {
            'sites_classified': len(sites_data),
            'active_count': active_count,
            'historic_count': historic_count,
            'cutoff_date': cutoff_date_str,
            'most_recent_date': most_recent_date
        }
        
    except Exception as e:
        logger.error(f"Error classifying active sites: {e}")
        return {'error': str(e), 'sites_classified': 0} 