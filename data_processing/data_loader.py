"""
Provides core utilities for loading, cleaning, and saving CSV data.

This module contains shared functions for handling data across the pipeline,
including standardizing site and column names, loading data from various
sources, and saving processed DataFrames.
"""

import os
import re
from difflib import SequenceMatcher

import pandas as pd

from data_processing import setup_logging
from database.database import close_connection, get_connection

logger = setup_logging("data_loader", category="processing")

RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'processed')
INTERIM_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'interim')

os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(INTERIM_DATA_DIR, exist_ok=True)

# Defines the file paths for each data source in the interim directory.
DATA_FILES = {
    'site': os.path.join(INTERIM_DATA_DIR, 'cleaned_site_data.csv'),  
    'chemical': os.path.join(INTERIM_DATA_DIR, 'cleaned_chemical_data.csv'),  
    'fish': os.path.join(INTERIM_DATA_DIR, 'cleaned_fish_data.csv'),  
    'macro': os.path.join(INTERIM_DATA_DIR, 'cleaned_macro_data.csv'),  
    'habitat': os.path.join(INTERIM_DATA_DIR, 'cleaned_habitat_data.csv'),
    'fish_collection_dates': os.path.join(INTERIM_DATA_DIR, 'cleaned_BT_fish_collection_dates.csv')
}

def get_file_path(data_type, processed=False):
    """
    Constructs the full file path for a given data type.
    
    Args:
        data_type: The type of data (e.g., 'site', 'chemical').
        processed: If True, returns the path to the processed file.
    
    Returns:
        The full path to the specified data file.
    """
    if not data_type in DATA_FILES:
        logger.error(f"Unknown data type: {data_type}")
        return None
    
    if processed:
        filename = f"processed_{data_type}_data.csv"
        return os.path.join(PROCESSED_DATA_DIR, filename)
    else:
        return DATA_FILES[data_type]

def check_file_exists(file_path):
    """
    Checks if a file exists at the given path.
    
    Args:
        file_path: The path to the file.
    
    Returns:
        True if the file exists, False otherwise.
    """
    exists = os.path.exists(file_path)
    if not exists:
        logger.error(f"File not found: {file_path}")
    return exists

def clean_site_name(site_name):
    """
    Standardizes site names by removing extra whitespace and normalizing format.
    
    Args:
        site_name: Raw site name from data file
        
    Returns:
        Cleaned site name string
    """
    if pd.isna(site_name) or site_name is None:
        return None
    
    # Convert to string and strip whitespace
    cleaned = str(site_name).strip()
    
    # Replace multiple whitespace with single space
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned

def clean_site_names_column(df, site_column='sitename', log_changes=True):
    """
    Cleans all site names within a specified column of a DataFrame.
    
    Args:
        df: The DataFrame containing the site name column.
        site_column: The name of the column with site names.
        log_changes: If True, logs any changes made to the site names.
    
    Returns:
        A DataFrame with the cleaned site names.
    """
    if site_column not in df.columns:
        logger.warning(f"Site column '{site_column}' not found in DataFrame")
        return df
    
    df_clean = df.copy()
    changes_made = 0
    
    for idx, original_name in df_clean[site_column].items():
        cleaned_name = clean_site_name(original_name)
        
        if pd.notna(original_name) and str(original_name) != str(cleaned_name):
            changes_made += 1
        
        df_clean.at[idx, site_column] = cleaned_name
    
    if log_changes and changes_made > 0:
        logger.info(f"Cleaned {changes_made} site names in {site_column} column")
    
    return df_clean

def load_csv_data(data_type, usecols=None, dtype=None, parse_dates=None, 
                  clean_site_names=True, encoding=None):
    """
    Loads data from a CSV file and optionally cleans the site name column.
    
    Args:
        data_type: The type of data to load.
        usecols: A list of columns to load.
        dtype: A dictionary of column data types.
        parse_dates: A list of columns to parse as dates.
        clean_site_names: If True, automatically standardizes site names.
        encoding: The encoding to use when reading the file.
    
    Returns:
        A DataFrame with the loaded data, or an empty DataFrame on failure.
    """
    file_path = get_file_path(data_type)
    
    if not file_path or not check_file_exists(file_path):
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(
            file_path,
            usecols=usecols,
            dtype=dtype,
            parse_dates=parse_dates,
            encoding=encoding,
            low_memory=False 
        )
        
        logger.info(f"Loaded {len(df)} rows from {data_type} data")
        
        if clean_site_names:
            # Attempt to find the site name column automatically.
            site_column = None
            for col in df.columns:
                if col.lower() in ['sitename', 'site_name', 'site name']:
                    site_column = col
                    break
            
            if site_column:
                df = clean_site_names_column(df, site_column, log_changes=True)
        
        return df
    
    except Exception as e:
        logger.error(f"Error loading {data_type} data: {e}")
        return pd.DataFrame()

def save_processed_data(df, data_type):
    """
    Saves a processed DataFrame to a CSV file in the processed data directory.
    
    Args:
        df: The DataFrame to save.
        data_type: A string used to identify the file, which will be sanitized.
    
    Returns:
        True if the file was saved successfully, False otherwise.
    """
    if df.empty:
        logger.warning(f"No {data_type} data to save")
        return False
    
    # Sanitize the data_type string to create a valid filename.
    sanitized_type = data_type
    for char in [' ', ':', ';', ',', '.', '/', '\\', '(', ')', '[', ']', '{', '}', '|', '*', '?', '&', '^', '%', '$', '#', '@', '!']:
        sanitized_type = sanitized_type.replace(char, '_')
    
    while '__' in sanitized_type:
        sanitized_type = sanitized_type.replace('__', '_')
    
    file_path = os.path.join(PROCESSED_DATA_DIR, f"processed_{sanitized_type}.csv")
    
    try:
        df.to_csv(file_path, index=False)
        logger.info(f"Saved {len(df)} rows of {data_type} data")
        return True
    
    except Exception as e:
        logger.error(f"Error saving processed {data_type} data: {e}")
        return False

def clean_column_names(df):
    """
    Standardizes all column names in a DataFrame.
    
    Converts names to lowercase and replaces special characters with underscores.
    
    Args:
        df: The DataFrame whose columns will be cleaned.
    
    Returns:
        A DataFrame with the standardized column names.
    """
    df_copy = df.copy()
    
    df_copy.columns = [
        col.replace(' \n', '_')
           .replace('\n', '_')
           .replace(' ', '_')
           .replace('-', '_')
           .replace('.', '')
           .replace('(', '')
           .replace(')', '')
           .lower()
        for col in df_copy.columns
    ]
    
    return df_copy

def filter_data_by_site(df, site_name, site_column='sitename'):
    """
    Filters a DataFrame to include only rows matching a specific site name.
    
    Args:
        df: The DataFrame to filter.
        site_name: The name of the site to filter by.
        site_column: The name of the column containing site names.
    
    Returns:
        A DataFrame containing only rows for the specified site.
    """
    clean_site_name_to_match = clean_site_name(site_name)
    
    if site_column not in df.columns:
        # Automatically find the site column if the provided one doesn't exist.
        site_columns = [col for col in df.columns if 'site' in col.lower()]
        if site_columns:
            site_column = site_columns[0]
            logger.info(f"Using {site_column} as the site column")
        else:
            logger.error(f"No site column found in DataFrame")
            return pd.DataFrame()
    
    filtered_df = df[df[site_column].apply(clean_site_name) == clean_site_name_to_match]
    
    if filtered_df.empty:
        logger.warning(f"No data found for site: {site_name}")
    else:
        logger.info(f"Found {len(filtered_df)} rows for site: {site_name}")
    
    return filtered_df

def get_unique_sites(data_type, site_column='sitename'):
    """
    Retrieves a list of unique site names from a given data file.
    
    Args:
        data_type: The type of data to load.
        site_column: The name of the column containing site names.
    
    Returns:
        A list of unique site names.
    """
    df = load_csv_data(data_type, clean_site_names=True)
    
    if df.empty:
        return []
    
    df = clean_column_names(df)
    
    if site_column not in df.columns:
        # Automatically find the site column if the provided one doesn't exist.
        site_columns = [col for col in df.columns if 'site' in col.lower()]
        if site_columns:
            site_column = site_columns[0]
        else:
            logger.error(f"No site column found in {data_type} data")
            return []
    
    unique_sites = df[site_column].dropna().unique().tolist()
    
    return unique_sites

def convert_bdl_values(df, bdl_columns, bdl_replacements):
    """
    Converts 'BDL' (Below Detection Limit) string values to numeric equivalents.
    
    Args:
        df: The DataFrame containing the data.
        bdl_columns: A list of columns that may contain 'BDL' values.
        bdl_replacements: A dictionary mapping column names to their BDL replacement values.
    
    Returns:
        A DataFrame with 'BDL' values converted to numbers.
    """
    df_copy = df.copy()
    
    for column in bdl_columns:
        if column in df_copy.columns:
            def convert_value(value):
                if isinstance(value, (int, float)):
                    return value
                elif isinstance(value, str) and value.upper() == 'BDL':
                    return bdl_replacements.get(column, 0)
                else:
                    try:
                        return float(value)
                    except:
                        return None
            
            df_copy[column] = df_copy[column].apply(convert_value)
    
    return df_copy

def get_date_range(data_type, date_column='Date'):
    """
    Retrieves the minimum and maximum dates from a given data file.
    
    Args:
        data_type: The type of data to load.
        date_column: The name of the column containing dates.
    
    Returns:
        A tuple containing the minimum and maximum dates, or (None, None).
    """
    if data_type == 'site':
        logger.info(f"Date range not applicable for site data")
        return None, None
        
    try:
        df = load_csv_data(data_type, parse_dates=[date_column], clean_site_names=False)
    except Exception as e:
        logger.error(f"Error loading {data_type} data for date range: {e}")
        return None, None
    
    if df.empty:
        return None, None
    
    df = clean_column_names(df)
    
    # After cleaning, the date column name will be lowercase.
    date_column_lower = date_column.lower()
    
    min_date = df[date_column_lower].min()
    max_date = df[date_column_lower].max()
    
    return min_date, max_date

def get_site_lookup_dict():
    """
    Creates a lookup dictionary of all sites in the database.
    
    Returns:
        Dictionary mapping site_name to site_id
    """
    conn = get_connection()
    try:
        sites_df = pd.read_sql_query("SELECT site_name, site_id FROM sites", conn)
        site_lookup = dict(zip(sites_df['site_name'], sites_df['site_id']))
        
        return site_lookup
    finally:
        close_connection(conn)

def find_site_id_by_name(site_name, strict=True):
    """
    Find site_id by name with optional fuzzy matching.
    
    Args:
        site_name: Site name from data file
        strict: If True, require exact match. If False, try fuzzy matching.
    
    Returns:
        Tuple of (site_id, match_type, confidence) where:
        - site_id: database site_id if found, None if not found
        - match_type: 'exact', 'fuzzy', or 'not_found'
        - confidence: matching confidence score (0.0-1.0)
    """
    if pd.isna(site_name) or site_name is None:
        logger.warning("Cannot match null/empty site name")
        return None, 'not_found', 0.0
    
    # Clean the input site name
    cleaned_name = clean_site_name(site_name)
    
    # Get all sites from database
    site_lookup = get_site_lookup_dict()
    
    # Try exact match first
    if cleaned_name in site_lookup:
        return site_lookup[cleaned_name], 'exact', 1.0
    
    # If strict mode, don't try fuzzy matching
    if strict:
        logger.warning(f"No exact match found for '{cleaned_name}' (strict mode)")
        return None, 'not_found', 0.0
    
    # Try fuzzy matching
    try:
        best_match = None
        best_score = 0.0
        min_similarity = 0.85  # Require 85% similarity for fuzzy match
        
        for db_site_name in site_lookup.keys():
            similarity = SequenceMatcher(None, cleaned_name.lower(), db_site_name.lower()).ratio()
            
            if similarity > best_score and similarity >= min_similarity:
                best_score = similarity
                best_match = db_site_name
        
        if best_match:
            return site_lookup[best_match], 'fuzzy', best_score
        else:
            return None, 'not_found', best_score
            
    except ImportError:
        logger.warning("difflib not available for fuzzy matching, falling back to exact match only")
        return None, 'not_found', 0.0

def validate_site_matches(df, site_name_column, strict=True, log_mismatches=True):
    """
    Validates that all sites in a DataFrame can be matched to database sites.
    
    Args:
        df: DataFrame containing site data
        site_name_column: Name of the column containing site names
        strict: Use strict matching (exact only) or allow fuzzy matching
        log_mismatches: If True, log detailed information about mismatches
    
    Returns:
        Dictionary with validation results and statistics
    """
    logger.info(f"Validating site matches for {len(df)} records (strict={strict})")
    
    if site_name_column not in df.columns:
        logger.error(f"Column '{site_name_column}' not found in DataFrame")
        return {
            'success': False,
            'error': f"Column '{site_name_column}' not found",
            'total_records': len(df),
            'matched_records': 0,
            'match_rate': 0.0
        }
    
    # Get unique site names to validate
    unique_sites = df[site_name_column].dropna().unique()
    
    validation_results = {
        'exact_matches': 0,
        'fuzzy_matches': 0,
        'no_matches': 0,
        'matched_sites': [],
        'unmatched_sites': []
    }
    
    for site_name in unique_sites:
        site_id, match_type, confidence = find_site_id_by_name(site_name, strict=strict)
        
        if match_type == 'exact':
            validation_results['exact_matches'] += 1
            validation_results['matched_sites'].append({
                'original_name': site_name,
                'matched_name': site_name,
                'site_id': site_id,
                'match_type': match_type,
                'confidence': confidence
            })
        elif match_type == 'fuzzy':
            validation_results['fuzzy_matches'] += 1
            # Get the actual matched name from database
            site_lookup = get_site_lookup_dict()
            matched_name = next((name for name, id in site_lookup.items() if id == site_id), site_name)
            validation_results['matched_sites'].append({
                'original_name': site_name,
                'matched_name': matched_name,
                'site_id': site_id,
                'match_type': match_type,
                'confidence': confidence
            })
        else:
            validation_results['no_matches'] += 1
            validation_results['unmatched_sites'].append({
                'original_name': site_name,
                'confidence': confidence
            })
    
    total_matched = validation_results['exact_matches'] + validation_results['fuzzy_matches']
    match_rate = total_matched / len(unique_sites) if unique_sites.size > 0 else 0
    
    # Log summary
    logger.info(f"Site validation: {len(unique_sites)} sites, {match_rate:.1%} match rate ({validation_results['exact_matches']} exact, {validation_results['fuzzy_matches']} fuzzy, {validation_results['no_matches']} unmatched)")
    
    # Log detailed mismatches if requested
    if log_mismatches and validation_results['unmatched_sites']:
        logger.warning(f"Unmatched sites: {[s['original_name'] for s in validation_results['unmatched_sites']]}")
    
    # Count records affected
    matched_records = len(df[df[site_name_column].isin([s['original_name'] for s in validation_results['matched_sites']])])
    
    return {
        'success': True,
        'total_records': len(df),
        'unique_sites': len(unique_sites),
        'matched_records': matched_records,
        'match_rate': match_rate,
        'validation_details': validation_results
    }

if __name__ == "__main__":
    print("Testing data loader:")
    
    for data_type in DATA_FILES.keys():
        if data_type == 'site':
            continue
            
        min_date, max_date = get_date_range(data_type)
        if min_date and max_date:
            print(f"  - Date range: {min_date} to {max_date}")
            
            df = load_csv_data(data_type)
            if not df.empty:
                print(f"  - Loaded {len(df)} rows and {len(df.columns)} columns")
                print(f"  - Column sample: {', '.join(df.columns[:5])}")
                
                sites = get_unique_sites(data_type)
                if sites:
                    print(f"  - Sample sites: {', '.join(sites[:5])}")
                    
                    min_date, max_date = get_date_range(data_type)
                    if min_date and max_date:
                        print(f"  - Date range: {min_date} to {max_date}")
            else:
                print(f"✗ Could not load {data_type} data")
        else:
            print(f"✗ {data_type.capitalize()} data file not found")
    
    print("\nData loader module test complete.")