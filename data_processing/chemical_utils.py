"""
Shared utilities for processing, validating, and inserting chemical water quality data.
"""

import sqlite3

import numpy as np
import pandas as pd

from data_processing import setup_logging
from database.database import close_connection, get_connection
from utils import round_parameter_value

logger = setup_logging("chemical_utils", category="processing")

# Constants and configuration

KEY_PARAMETERS = [    
    'do_percent', 'pH', 'soluble_nitrogen', 
    'Phosphorus', 'Chloride', 
]

PARAMETER_MAP = {
    'do_percent': 1,
    'pH': 2,
    'soluble_nitrogen': 3,
    'Phosphorus': 4,
    'Chloride': 5
}

# BDL (Below Detection Limit) values provided by the Blue Thumb Coordinator.
BDL_VALUES = {
    'Nitrate': 0.3,    
    'Nitrite': 0.03,    
    'Ammonia': 0.03,
    'Phosphorus': 0.005,
}

# Data validation and cleaning

def convert_bdl_value(value, bdl_replacement):
    """
    Converts a zero value to its BDL replacement value.
    
    This function treats zero as an indicator for a value that is below the
    detection limit. It keeps NaN values as they are to represent actual gaps
    in data for visualization purposes.
    
    Args:
        value: The value to check and convert.
        bdl_replacement: The value to use for BDL replacement.
        
    Returns:
        The converted value, or NaN if the input was NaN or could not be converted.
    """
    if pd.isna(value):
        return np.nan  # Preserve NaN to represent data gaps.
    
    try:
        if not isinstance(value, (int, float)):
            value = float(value)
    except:
        return np.nan  
        
    if value == 0:
        return bdl_replacement  # Assumes that a value of zero means it is below the detection limit.
    
    return value

def validate_chemical_data(df, remove_invalid=True):
    """
    Validates chemical data, ensuring values are within logical ranges.
    
    Checks that pH is between 0-14 and all other chemical parameters are non-negative.
    Invalid values can either be removed (set to NaN) or logged as warnings.
    
    Args:
        df: A DataFrame containing the chemical data.
        remove_invalid: If True, sets invalid values to NaN.
        
    Returns:
        A DataFrame with the validated data.
    """
    df_clean = df.copy()
    total_issues = 0
    
    # pH has a specific valid range (0-14).
    chemical_params = ['do_percent', 'Nitrate', 'Nitrite', 'Ammonia', 
                      'Phosphorus', 'Chloride', 'soluble_nitrogen']
    
    if 'pH' in df_clean.columns:
        ph_invalid_mask = ((df_clean['pH'] < 0) | (df_clean['pH'] > 14)) & df_clean['pH'].notna()
        ph_invalid_count = ph_invalid_mask.sum()
        
        if ph_invalid_count > 0:
            total_issues += ph_invalid_count
            if remove_invalid:
                df_clean.loc[ph_invalid_mask, 'pH'] = np.nan
                logger.info(f"Removed {ph_invalid_count} pH values outside 0-14 range")
            else:
                logger.warning(f"Found {ph_invalid_count} pH values outside 0-14 range")
    
    # Other chemical parameters must be non-negative.
    for param in chemical_params:
        if param in df_clean.columns:
            invalid_mask = (df_clean[param] < 0) & df_clean[param].notna()
            invalid_count = invalid_mask.sum()
            
            if invalid_count > 0:
                total_issues += invalid_count
                if remove_invalid:
                    df_clean.loc[invalid_mask, param] = np.nan
                    logger.info(f"Removed {invalid_count} {param} values < 0")
                else:
                    logger.warning(f"Found {invalid_count} {param} values < 0")
    
    if total_issues > 0:
        action = "removed" if remove_invalid else "flagged"
        logger.info(f"Data validation complete: {total_issues} total issues {action}")
    else:
        logger.info("Data validation complete: No quality issues found")
    
    return df_clean

def apply_bdl_conversions(df, bdl_columns=None):
    """
    Applies BDL conversions to the specified DataFrame columns.
    
    Args:
        df: The DataFrame with chemical data.
        bdl_columns: A list of columns to apply BDL conversions to.
        
    Returns:
        A DataFrame with BDL conversions applied.
    """
    if bdl_columns is None:
        bdl_columns = list(BDL_VALUES.keys())
    
    df_converted = df.copy()
    conversion_count = 0
    
    for column in bdl_columns:
        if column in df_converted.columns:
            bdl_value = BDL_VALUES.get(column, 0)
            
            df_converted[column] = df_converted[column].apply(
                lambda x: convert_bdl_value(x, bdl_value)
            )
            conversion_count += 1
    
    if conversion_count > 0:
        logger.info(f"Applied BDL conversions to {conversion_count} columns")
    
    return df_converted

def remove_empty_chemical_rows(df, chemical_columns=None):
    """
    Removes rows from a DataFrame where all specified chemical columns are null.
    
    Args:
        df: The DataFrame to process.
        chemical_columns: A list of columns to check for null values.
        
    Returns:
        A DataFrame with empty rows removed.
    """
    if chemical_columns is None:
        chemical_columns = ['do_percent', 'pH', 'Nitrate', 'Nitrite', 'Ammonia', 
                           'Phosphorus', 'Chloride', 'soluble_nitrogen']
    
    existing_columns = [col for col in chemical_columns if col in df.columns]
    
    if not existing_columns:
        logger.warning("No chemical columns found for empty row removal")
        return df
    
    non_null_counts = df[existing_columns].notnull().sum(axis=1)
    
    df_filtered = df[non_null_counts > 0].copy()
    
    removed_count = len(df) - len(df_filtered)
    if removed_count > 0:
        logger.info(f"Removed {removed_count} rows with no chemical data")
    
    return df_filtered

# Data processing and analysis

def calculate_soluble_nitrogen(df):
    """
    Calculates soluble nitrogen by summing Nitrate, Nitrite, and Ammonia values.
    
    This function uses BDL replacement values for calculations to handle nulls and
    zeros consistently, ensuring that the total soluble nitrogen is a conservative
    and accurate representation.
    
    Args:
        df: A DataFrame containing the individual nitrogen component columns.
        
    Returns:
        The DataFrame with a 'soluble_nitrogen' column added.
    """
    try:
        required_columns = ['Nitrate', 'Nitrite', 'Ammonia']
        if not all(col in df.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df.columns]
            logger.warning(f"Cannot calculate soluble_nitrogen: Missing columns: {', '.join(missing)}")
            return df
        
        df_calc = df.copy()
        
        # For calculation, treat NaNs and zeros as their respective BDL values.
        def get_calc_value(series, bdl_value):
            return series.fillna(bdl_value).replace(0, bdl_value).infer_objects(copy=False)
        
        nitrate_calc = get_calc_value(df_calc['Nitrate'], BDL_VALUES['Nitrate'])
        nitrite_calc = get_calc_value(df_calc['Nitrite'], BDL_VALUES['Nitrite'])
        ammonia_calc = get_calc_value(df_calc['Ammonia'], BDL_VALUES['Ammonia'])
        
        df_calc['soluble_nitrogen'] = nitrate_calc + nitrite_calc + ammonia_calc
        
        # Apply rounding to ensure consistent decimal places for reporting.
        df_calc['soluble_nitrogen'] = df_calc['soluble_nitrogen'].apply(
            lambda x: float(f"{x:.2f}") if pd.notna(x) else x
        )
        
        logger.info("Successfully calculated soluble_nitrogen from component values")
        return df_calc
        
    except Exception as e:
        logger.error(f"Error calculating soluble_nitrogen: {e}")
        return df

def determine_status(parameter, value, reference_values):
    """
    Determines the status of a parameter value based on reference thresholds.
    
    Args:
        parameter: The name of the parameter (e.g., 'do_percent', 'pH').
        value: The parameter value to evaluate.
        reference_values: A dictionary of reference thresholds for parameters.
        
    Returns:
        A string representing the status ('Normal', 'Caution', 'Poor', etc.).
    """
    if pd.isna(value):
        return "Unknown"
        
    if parameter not in reference_values:
        return "Normal"  # Default to Normal if no reference values are defined.
        
    ref = reference_values[parameter]
    
    if parameter == 'do_percent':
        if 'normal min' in ref and 'normal max' in ref:
            if 'caution min' in ref and 'caution max' in ref:
                if value < ref['caution min'] or value > ref['caution max']:
                    return "Poor"
                elif value < ref['normal min'] or value > ref['normal max']:
                    return "Caution"
                else:
                    return "Normal"
                    
    elif parameter == 'pH':
        if 'normal min' in ref and 'normal max' in ref:
            if value < ref['normal min']:
                return "Below Normal (Acidic)"
            elif value > ref['normal max']:
                return "Above Normal (Basic/Alkaline)"
            else:
                return "Normal"
                
    elif parameter in ['soluble_nitrogen', 'Phosphorus', 'Chloride']:
        if 'caution' in ref and 'normal' in ref:
            if value > ref['caution']:
                return "Poor"
            elif value > ref['normal']:
                return "Caution"
            else:
                return "Normal"
                
    return "Normal"

def get_reference_values():
    """
    Retrieves chemical reference values from the database.
    
    Returns:
        A dictionary of reference values, organized by parameter.
        
    Raises:
        Exception: If reference values cannot be retrieved from the database.
    """
    conn = get_connection()
    try:
        reference_values = {}
        
        query = """
        SELECT p.parameter_code, r.threshold_type, r.value
        FROM chemical_reference_values r
        JOIN chemical_parameters p ON r.parameter_id = p.parameter_id
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            raise Exception("No chemical reference values found in database. Database initialization may have failed.")
        
        for param in df['parameter_code'].unique():
            reference_values[param] = {}
            param_data = df[df['parameter_code'] == param]
            
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
            
        return reference_values
        
    except Exception as e:
        logger.error(f"Error getting reference values: {e}")
        raise Exception(f"Critical error: Cannot retrieve chemical reference values from database: {e}")
    finally:
        close_connection(conn)

# Database operations

def get_existing_data(conn):
    """
    Retrieves all existing chemical measurements and site lookups for batch processing.
    
    Returns:
        A tuple containing a set of existing measurements and a site lookup dictionary.
    """
    existing_measurements_query = """
    SELECT event_id, parameter_id
    FROM chemical_measurements
    """
    existing_measurements_df = pd.read_sql_query(existing_measurements_query, conn)
    existing_measurements = set(zip(existing_measurements_df['event_id'], existing_measurements_df['parameter_id']))
    
    # Sites are not created here; this is only for looking up existing sites.
    existing_sites_df = pd.read_sql_query("SELECT site_name, site_id FROM sites", conn)
    site_lookup = dict(zip(existing_sites_df['site_name'], existing_sites_df['site_id']))
    
    return existing_measurements, site_lookup

def insert_collection_event(cursor, site_id, date_str, year, month, site_name, sample_id=None, return_inserted=False):
    """
    Inserts a new chemical collection event, allowing for duplicate site-date entries.
    
    Args:
        cursor: A database cursor.
        site_id: The ID of the site (must already exist).
        date_str: The collection date as a 'YYYY-MM-DD' string.
        year: The year of collection.
        month: The month of collection.
        site_name: The name of the site, used for logging.
        
    Returns:
        The event_id of the newly created event.
    """
    if sample_id is not None and not pd.isna(sample_id):
        try:
            sample_id_int = int(sample_id)
        except Exception:
            sample_id_int = None
    else:
        sample_id_int = None

    if sample_id_int is not None:
        cursor.execute(
            "SELECT event_id FROM chemical_collection_events WHERE sample_id = ?",
            (sample_id_int,),
        )
        existing = cursor.fetchone()
        if existing and existing[0] is not None:
            if return_inserted:
                return existing[0], False
            return existing[0]

        try:
            cursor.execute(
                """
                INSERT INTO chemical_collection_events 
                (site_id, sample_id, collection_date, year, month)
                VALUES (?, ?, ?, ?, ?)
                """,
                (site_id, sample_id_int, date_str, year, month),
            )
        except sqlite3.IntegrityError:
            cursor.execute(
                "SELECT event_id FROM chemical_collection_events WHERE sample_id = ?",
                (sample_id_int,),
            )
            existing_after = cursor.fetchone()
            if existing_after and existing_after[0] is not None:
                if return_inserted:
                    return existing_after[0], False
                return existing_after[0]
            raise
        if return_inserted:
            return cursor.lastrowid, True
        return cursor.lastrowid

    cursor.execute(
        """
        INSERT INTO chemical_collection_events 
        (site_id, collection_date, year, month)
        VALUES (?, ?, ?, ?)
        """,
        (site_id, date_str, year, month),
    )

    if return_inserted:
        return cursor.lastrowid, True
    return cursor.lastrowid

def insert_chemical_measurement(cursor, event_id, parameter_id, value, status, existing_measurements):
    """
    Inserts a new chemical measurement if it does not already exist for the event.
    
    Args:
        cursor: A database cursor.
        event_id: The ID of the collection event.
        parameter_id: The ID of the parameter.
        value: The measured value.
        status: The status of the measurement ('Normal', 'Caution', etc.).
        existing_measurements: A set of existing (event_id, parameter_id) tuples.
        
    Returns:
        True if the measurement was inserted, False if it already existed.
    """
    if (event_id, parameter_id) not in existing_measurements:
        cursor.execute("""
        INSERT INTO chemical_measurements
        (event_id, parameter_id, value, status)
        VALUES (?, ?, ?, ?)
        """, (event_id, parameter_id, value, status))
        
        existing_measurements.add((event_id, parameter_id))
        return True
    return False

def insert_chemical_data(df, allow_duplicates=True, data_source="unknown"):
    """
    Inserts processed chemical data into the database in a batch operation.
    
    This function allows for duplicate site-date combinations by default, preserving
    all original chemical data including any replicate samples.
 
    Args:
        df: A DataFrame with processed chemical data.
        allow_duplicates: If True, allows multiple events for the same site and date.
        data_source: A string describing the source of the data for logging.
        
    Returns:
        A dictionary of statistics about the insertion process.
    """
    if df.empty:
        logger.warning(f"No data to process for {data_source}")
        return {
            'sites_processed': 0,
            'events_added': 0,
            'measurements_added': 0,
            'data_source': data_source
        }
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        reference_values = get_reference_values()
        
        existing_measurements, site_lookup = get_existing_data(conn)
        
        stats = {
            'sites_processed': 0,
            'events_added': 0,
            'measurements_added': 0,
            'data_source': data_source
        }
        
        for (site_name, date), group in df.groupby(['Site_Name', 'Date']):
            stats['sites_processed'] += 1
            
            # Sites are guaranteed to exist from prior processing steps.
            site_id = site_lookup[site_name]
            
            # Each site-date group is processed as a unique collection event.
            for _, row in group.iterrows():
                date_str = row['Date'].strftime('%Y-%m-%d')
                year = row['Year']
                month = row['Month']
                sample_id = None
                if 'sample_id' in row and pd.notna(row['sample_id']):
                    sample_id = row['sample_id']
                
                event_id, event_was_inserted = insert_collection_event(
                    cursor,
                    site_id,
                    date_str,
                    year,
                    month,
                    site_name,
                    sample_id=sample_id,
                    return_inserted=True,
                )
                if event_was_inserted:
                    stats['events_added'] += 1
                
                for param_name, param_id in PARAMETER_MAP.items():
                    if param_name in row and pd.notna(row[param_name]):
                        raw_value = row[param_name]
                        
                        rounded_value = round_parameter_value(param_name, raw_value, 'chemical')
                        
                        if rounded_value is None:
                            continue
                            
                        status = determine_status(param_name, rounded_value, reference_values)
                        
                        measurement_was_inserted = insert_chemical_measurement(
                            cursor, event_id, param_id, rounded_value, status, existing_measurements
                        )
                        if measurement_was_inserted:
                            stats['measurements_added'] += 1
        
        conn.commit()
        
        logger.info(f"Successfully inserted {data_source}: {stats['measurements_added']} measurements from {stats['sites_processed']} sites")
        
        return stats
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error in batch insertion for {data_source}: {e}")
        raise Exception(f"Failed to insert {data_source} data: {e}")
    finally:
        close_connection(conn)

