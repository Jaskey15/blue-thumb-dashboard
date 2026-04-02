"""
Processes updated chemical data with complex, multi-range readings.

This module handles a specific chemical data format where nutrients are
measured across low, mid, or high ranges. It includes logic to select the
appropriate value based on a "range selection" column and formats the
final data for database insertion.
"""

import os

import pandas as pd

from data_processing import setup_logging
from data_processing.chemical_utils import (
    apply_bdl_conversions,
    calculate_soluble_nitrogen,
    insert_chemical_data,
    remove_empty_chemical_rows,
    validate_chemical_data,
)

logger = setup_logging("updated_chemical_processing", category="processing")

# Defines the column mappings for nutrients that have multiple measurement ranges.
NUTRIENT_COLUMN_MAPPINGS = {
    'ammonia': {
        'range_selection': 'Ammonia Nitrogen Range Selection',
        'low_col1': 'Ammonia Nitrogen Low Reading #1',
        'low_col2': 'Ammonia Nitrogen Low Reading #2', 
        'mid_col1': 'Ammonia_nitrogen_midrange1_Final',
        'mid_col2': 'Ammonia_nitrogen_midrange2_Final'
    },
    'orthophosphate': {
        'range_selection': 'Orthophosphate Range Selection',
        'low_col1': 'Orthophosphate_Low1_Final',
        'low_col2': 'Orthophosphate_Low2_Final',
        'mid_col1': 'Orthophosphate_Mid1_Final', 
        'mid_col2': 'Orthophosphate_Mid2_Final',
        'high_col1': 'Orthophosphate_High1_Final',
        'high_col2': 'Orthophosphate_High2_Final'
    },
    'chloride': {
        'range_selection': 'Chloride Range Selection',
        'low_col1': 'Chloride_Low1_Final',
        'low_col2': 'Chloride_Low2_Final',
        'high_col1': 'Chloride_High1_Final',
        'high_col2': 'Chloride_High2_Final'
    }
}

def load_updated_chemical_data():
    """
    Loads the cleaned, updated chemical data from its interim CSV file.
    
    Returns:
        A DataFrame containing the raw, updated chemical data.
    """
    try:
        cleaned_file_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 
            'data', 'interim', 'cleaned_updated_chemical_data.csv'
        )
        
        if not os.path.exists(cleaned_file_path):
            logger.error("cleaned_updated_chemical_data.csv not found. Run CSV cleaning first.")
            return pd.DataFrame()
        
        df = pd.read_csv(cleaned_file_path, low_memory=False) 
        
        logger.info(f"Successfully loaded {len(df)} rows from cleaned updated chemical data")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading cleaned updated chemical data: {e}")
        return pd.DataFrame()

def parse_sampling_dates(df):
    """
    Extracts the date from a 'Sampling Date' column containing date and time.
    
    Args:
        df: A DataFrame with a 'Sampling Date' column.
        
    Returns:
        The DataFrame with a parsed 'Date' column.
    """
    try:
        # The source format includes both date and time, but only the date is needed.
        df['parsed_datetime'] = pd.to_datetime(df['Sampling Date'], format='%m/%d/%Y, %I:%M %p')
        
        df['Date'] = df['parsed_datetime'].dt.date
        
        # Convert back to datetime for consistency with other data processing steps.
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Add year and month for easier filtering and analysis.
        df['Year'] = df['Date'].dt.year
        df['Month'] = df['Date'].dt.month
        
        logger.info(f"Successfully parsed {len(df)} dates")
        logger.info(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
        
        df = df.drop(columns=['parsed_datetime'])
        
        return df
        
    except Exception as e:
        logger.error(f"Error parsing sampling dates: {e}")
        return df

def get_greater_value(row, col1, col2, tiebreaker='col1'):
    """
    Selects the greater of two numeric values from a row.
    
    This handles cases where one or both values might be null or non-numeric.
    
    Args:
        row: A row from a Pandas DataFrame.
        col1: The name of the first column to compare.
        col2: The name of the second column to compare.  
        tiebreaker: Which column to prefer if values are equal.
        
    Returns:
        The greater numeric value, or None if both are invalid.
    """
    try:
        val1 = pd.to_numeric(row[col1], errors='coerce') if pd.notna(row[col1]) else None
        val2 = pd.to_numeric(row[col2], errors='coerce') if pd.notna(row[col2]) else None
        
        if val1 is None and val2 is None:
            return None
            
        if val1 is None:
            return val2
        if val2 is None:
            return val1
            
        # If both values are valid, return the greater one.
        if val1 > val2:
            return val1
        elif val2 > val1:
            return val2
        else:  
            return val1 if tiebreaker == 'col1' else val2
            
    except Exception as e:
        logger.warning(f"Error comparing {col1} and {col2}: {e}")
        return None

def get_conditional_nutrient_value(row, range_selection_col, low_col1, low_col2, mid_col1=None, mid_col2=None, high_col1=None, high_col2=None):
    """
    Selects a nutrient value based on a specified measurement range.
    
    Args:
        row: A row from a Pandas DataFrame.
        range_selection_col: The column indicating which range to use ('Low', 'Mid', 'High').
        low_col1, low_col2: Columns for the low-range readings.
        mid_col1, mid_col2: Optional columns for the mid-range readings.
        high_col1, high_col2: Optional columns for the high-range readings.
        
    Returns:
        The selected nutrient value, or None if no valid reading is found.
    """
    try:
        range_selection = row[range_selection_col]
        
        if pd.isna(range_selection) or range_selection == '':
            return None
            
        range_selection = str(range_selection).strip()
        
        # Select the correct pair of columns based on the range identifier.
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
    """
    Applies the conditional nutrient logic for a specified nutrient.
    
    Args:
        df: The DataFrame containing nutrient columns.
        nutrient_name: The key from the NUTRIENT_COLUMN_MAPPINGS dictionary.
        
    Returns:
        A Pandas Series with the processed nutrient values.
    """
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
            high_col2=mapping.get('high_col2')
        ), axis=1)
        
        logger.info(f"Successfully processed {nutrient_name} values")
        return result
        
    except Exception as e:
        logger.error(f"Error processing {nutrient_name}: {e}")
        return pd.Series([None] * len(df))

def process_simple_nutrients(df):
    """
    Processes nutrients that only require selecting the greater of two values.
    
    Args:
        df: The DataFrame containing nutrient columns.
        
    Returns:
        The DataFrame with processed 'Nitrate' and 'Nitrite' columns.
    """
    try:
        df['Nitrate'] = df.apply(lambda row: get_greater_value(row, 'Nitrate #1', 'Nitrate #2'), axis=1)
        
        df['Nitrite'] = df.apply(lambda row: get_greater_value(row, 'Nitrite #1', 'Nitrite #2'), axis=1)
        
        logger.info("Successfully processed Nitrate and Nitrite values")
        return df
        
    except Exception as e:
        logger.error(f"Error processing simple nutrients: {e}")
        return df

def get_ph_worst_case(row):
    """
    Selects the pH value that is furthest from the neutral value of 7.

    This approach is used because deviations in either direction (acidic or
    basic) are equally significant for water quality assessment.

    Args:
        row: A row from a Pandas DataFrame.

    Returns:
        The selected pH value, or None if no valid readings exist.
    """
    try:
        ph1 = pd.to_numeric(row['pH #1'], errors='coerce')
        ph2 = pd.to_numeric(row['pH #2'], errors='coerce')

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

def format_to_database_schema(df):
    """
    Formats the processed data to align with the database schema.
    
    This involves renaming columns, calculating derived values like pH and
    soluble nitrogen, and selecting the final set of columns for insertion.
    
    Args:
        df: The DataFrame with processed nutrient data.
        
    Returns:
        A DataFrame formatted and ready for database insertion.
    """
    try:
        formatted_df = df.copy()
        has_sample_id = 'sample_id' in formatted_df.columns
        
        # Calculate final pH using the "worst-case" (furthest from 7) logic.
        formatted_df['pH'] = formatted_df.apply(get_ph_worst_case, axis=1)

        # Rename columns to match the existing database schema.
        column_mappings = {
            'Site Name': 'Site_Name',
            '% Oxygen Saturation': 'do_percent',
            'Orthophosphate': 'Phosphorus'
        }
        
        formatted_df = formatted_df.rename(columns=column_mappings)
        
        # Calculate soluble nitrogen from its components.
        formatted_df = calculate_soluble_nitrogen(formatted_df)
        
        # Select and order columns to match the database table exactly.
        required_columns = ['Site_Name', 'Date', 'Year', 'Month', 'do_percent', 'pH', 
                           'Nitrate', 'Nitrite', 'Ammonia', 'Phosphorus', 'Chloride', 
                           'soluble_nitrogen']
        if has_sample_id:
            required_columns.append('sample_id')

        # Preserve geometry columns for cloud sync site resolution
        for geo_col in ('latitude', 'longitude'):
            if geo_col in formatted_df.columns:
                required_columns.append(geo_col)

        formatted_df = formatted_df[required_columns]
        
        # Ensure all final data columns are in a numeric format.
        numeric_columns = ['do_percent', 'pH', 'Nitrate', 'Nitrite', 'Ammonia', 
                          'Phosphorus', 'Chloride', 'soluble_nitrogen']
        
        for col in numeric_columns:
            formatted_df[col] = pd.to_numeric(formatted_df[col], errors='coerce')
        
        logger.info(f"Successfully formatted {len(formatted_df)} rows to database schema")
        logger.info(f"Final columns: {list(formatted_df.columns)}")
        
        return formatted_df
        
    except Exception as e:
        logger.error(f"Error formatting data to database schema: {e}")
        return pd.DataFrame()

def process_updated_chemical_data():
    """
    Executes the full processing pipeline for the updated chemical data.
    
    Returns:
        A DataFrame with the fully processed and validated data.
    """
    try:
        logger.info("Starting complete processing of updated chemical data...")
        
        df = load_updated_chemical_data()
        if df.empty:
            return pd.DataFrame()
        
        df = parse_sampling_dates(df)
        
        # Process nutrients with different logic.
        df = process_simple_nutrients(df)
        df['Ammonia'] = process_conditional_nutrient(df, 'ammonia')
        df['Orthophosphate'] = process_conditional_nutrient(df, 'orthophosphate') 
        df['Chloride'] = process_conditional_nutrient(df, 'chloride')
        
        # Standardize the data to match the database schema.
        formatted_df = format_to_database_schema(df)
        
        # Apply final cleaning, validation, and BDL conversions.
        formatted_df = remove_empty_chemical_rows(formatted_df)
        formatted_df = validate_chemical_data(formatted_df, remove_invalid=True)
        
        formatted_df = apply_bdl_conversions(formatted_df)
        
        logger.info(f"Complete processing finished: {len(formatted_df)} rows ready for database")
        return formatted_df
        
    except Exception as e:
        logger.error(f"Error in complete processing pipeline: {e}")
        return pd.DataFrame()

def load_updated_chemical_data_to_db():
    """
    Processes the updated chemical data and loads it into the database.
    
    Returns:
        True if the pipeline runs successfully, False otherwise.
    """
    try:
        logger.info("Starting complete pipeline for updated chemical data...")
        
        processed_df = process_updated_chemical_data()
        
        if processed_df.empty:
            logger.error("Failed to process updated chemical data")
            return False
        
        logger.info(f"Successfully processed {len(processed_df)} records")
        
        # Use the shared utility for batch insertion into the database.
        logger.info(f"Inserting {len(processed_df)} records into database...")
        
        stats = insert_chemical_data(
            processed_df,
            data_source="cleaned_updated_chemical_data.csv"
        )
        
        logger.info("Successfully completed updated chemical data pipeline!")
        logger.info(f"Final summary:")
        logger.info(f"  - Processed: {len(processed_df)} total records")
        logger.info(f"  - Records inserted: {stats['measurements_added']}")
        
        return True
            
    except Exception as e:
        logger.error(f"Error in updated chemical data pipeline: {e}")
        return False