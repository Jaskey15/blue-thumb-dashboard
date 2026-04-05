"""
Cleans raw data and consolidates site information from multiple CSV files.

This script runs a two-phase pipeline:
1.  Cleans all raw CSV files by standardizing site names.
2.  Consolidates sites into a master list, taking metadata from the
    highest-priority source file available and flagging any conflicts.
"""

import os
import sys

import pandas as pd

from data_processing import setup_logging
from data_processing.data_loader import clean_site_name

logger = setup_logging("consolidate_sites", category="processing")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
INTERIM_DATA_DIR = os.path.join(BASE_DIR, 'data', 'interim')
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')

os.makedirs(INTERIM_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# CSV files ordered by priority (1-3 high, 5-6 low).
# Priority 4 is the Feature Server, handled separately in consolidate_sites().
CSV_CONFIGS_HIGH = [
    {
        'file': 'cleaned_site_data.csv',
        'site_column': 'SiteName',
        'lat_column': 'Latitude',
        'lon_column': 'Longitude',
        'county_column': 'County',
        'basin_column': 'RiverBasin',
        'ecoregion_column': 'Mod_Ecoregion',
        'description': 'Master site data'
    },
    {
        'file': 'cleaned_chemical_data.csv',
        'site_column': 'SiteName',
        'lat_column': 'Latitude',
        'lon_column': 'Longitude',
        'county_column': 'County',
        'basin_column': 'RiverBasin',
        'ecoregion_column': None,  # Not available in chemical data
        'description': 'Original chemical data'
    },
    {
        'file': 'cleaned_fish_data.csv',
        'site_column': 'SiteName',
        'lat_column': 'Latitude',
        'lon_column': 'Longitude',
        'county_column': None,  # Not readily available
        'basin_column': 'RiverBasin',
        'ecoregion_column': 'Mod_Ecoregion',
        'description': 'Fish community data'
    },
]

CSV_CONFIGS_LOW = [
    {
        'file': 'cleaned_macro_data.csv',
        'site_column': 'SiteName',
        'lat_column': 'Latitude',
        'lon_column': 'Longitude',
        'county_column': None,  # Not available
        'basin_column': None,  # Not available
        'ecoregion_column': 'Mod_Ecoregion',
        'description': 'Macroinvertebrate data'
    },
    {
        'file': 'cleaned_habitat_data.csv',
        'site_column': 'SiteName',
        'lat_column': None,  # Not available in habitat data
        'lon_column': None,  # Not available in habitat data
        'county_column': None,
        'basin_column': 'RiverBasin',
        'ecoregion_column': None,
        'description': 'Habitat assessment data'
    }
]

# Combined list for verification and iteration (excludes Feature Server source).
CSV_CONFIGS = CSV_CONFIGS_HIGH + CSV_CONFIGS_LOW

def clean_all_csvs():
    """
    Cleans all raw CSVs by standardizing site names and saves them to the interim directory.
    
    Returns:
        True if all files were processed successfully, False otherwise.
    """
    logger.info("=" * 60)
    logger.info("PHASE 1: CSV CLEANING")
    logger.info("=" * 60)
    logger.info(f"Input: {RAW_DATA_DIR}")
    logger.info(f"Output: {INTERIM_DATA_DIR}")
    
    csv_files = [
        'site_data.csv',
        'chemical_data.csv',
        'fish_data.csv',
        'macro_data.csv',
        'habitat_data.csv',
    ]
    
    total_changes = 0
    total_sites = 0
    processed_files = []
    
    for input_file in csv_files:
        try:
            site_column = 'SiteName'
            encoding = None
            
            output_file = f'cleaned_{input_file}'
            description = input_file.replace('_', ' ').replace('.csv', ' data')
            
            input_path = os.path.join(RAW_DATA_DIR, input_file)
            output_path = os.path.join(INTERIM_DATA_DIR, output_file)
            
            logger.info(f"Processing {description}: {input_file}")
            
            if encoding:
                df = pd.read_csv(input_path, encoding=encoding, low_memory=False)
            else:
                df = pd.read_csv(input_path, low_memory=False)
            
            original_sites = df[site_column].copy()
            df[site_column] = df[site_column].str.strip().str.replace(r'\s+', ' ', regex=True)
            
            changes_mask = (original_sites != df[site_column]) & original_sites.notna()
            site_changes = changes_mask.sum()
            
            df.to_csv(output_path, index=False, encoding='utf-8')
            
            unique_sites = df[site_column].nunique()
            if site_changes > 0:
                logger.info(f"  ✓ {len(df)} rows, {site_changes} names cleaned, {unique_sites} unique sites")
            else:
                logger.info(f"  ✓ {len(df)} rows, {unique_sites} unique sites")
            
            total_changes += site_changes
            total_sites += unique_sites
            processed_files.append(output_file)
            
        except Exception as e:
            logger.error(f"Failed to process {input_file}: {e}")
            return False
    
    logger.info(f"\n🎉 Successfully cleaned all {len(processed_files)} CSV files!")
    logger.info(f"Total: {total_changes} site name changes, {total_sites} unique sites across all files")
    
    logger.info("\nCleaned files created:")
    for filename in processed_files:
        logger.info(f"  - {filename}")
    
    return True

def extract_sites_from_csv(config):
    """
    Extracts unique sites and their metadata from a single cleaned CSV file.
    
    Args:
        config: A dictionary defining the configuration for the CSV file.
        
    Returns:
        A DataFrame containing unique sites and their associated metadata.
    """
    file_path = os.path.join(INTERIM_DATA_DIR, config['file'])
    
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {config['file']}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(file_path, low_memory=False)
        
        if config['site_column'] not in df.columns:
            logger.error(f"Site column '{config['site_column']}' not found in {config['file']}")
            return pd.DataFrame()
        
        unique_sites = df.drop_duplicates(subset=[config['site_column']])
        
        site_data = pd.DataFrame()
        site_data['site_name'] = unique_sites[config['site_column']].apply(clean_site_name)
        
        for metadata_field, column_name in [
            ('latitude', config['lat_column']),
            ('longitude', config['lon_column']),
            ('county', config['county_column']),
            ('river_basin', config['basin_column']),
            ('ecoregion', config['ecoregion_column'])
        ]:
            if column_name and column_name in df.columns:
                site_data[metadata_field] = unique_sites[column_name]
            else:
                site_data[metadata_field] = None
        
        site_data['source_file'] = config['file']
        site_data['source_description'] = config['description']
        
        site_data = site_data[site_data['site_name'].notna() & (site_data['site_name'] != '')]
        
        return site_data
        
    except Exception as e:
        logger.error(f"Error processing {config['file']}: {e}")
        return pd.DataFrame()

def detect_conflicts(site_name, existing_site, new_site):
    """
    Compares two site records and identifies any conflicting metadata.
    
    A conflict is flagged only when a metadata field has a non-null but different
    value in both the existing and new records.
    
    Args:
        site_name: The name of the site being compared.
        existing_site: A pandas Series with the existing site data.
        new_site: A pandas Series with the new site data.
        
    Returns:
        A list of strings describing any conflicts found.
    """
    conflicts = []
    
    for field in ['latitude', 'longitude', 'county', 'river_basin', 'ecoregion']:
        existing_val = existing_site.get(field)
        new_val = new_site.get(field)
        
        # A conflict occurs only if both values exist and are different.
        if (pd.notna(existing_val) and pd.notna(new_val) and 
            existing_val != new_val):
            conflicts.append(f"{field}: '{existing_val}' vs '{new_val}'")
    
    return conflicts

def consolidate_sites():
    """
    Consolidates site information from all cleaned CSV files based on priority.
    
    This function iterates through the CSV files in their priority order, adding new
    sites and updating existing ones with metadata from higher-priority sources.
    It also identifies and logs any conflicting metadata for manual review.
    
    Returns:
        A tuple containing the consolidated sites DataFrame and a DataFrame of any conflicts found.
    """
    logger.info("\n" + "=" * 60)
    logger.info("PHASE 2: SITE CONSOLIDATION")
    logger.info("=" * 60)
    logger.info("Starting site consolidation process...")
    
    consolidated_sites = pd.DataFrame()
    conflicts_list = []

    def _merge_sites(site_df, priority_label):
        """Merge a DataFrame of sites into consolidated_sites."""
        nonlocal consolidated_sites
        sites_added = 0
        sites_updated = 0
        conflicts_found = 0

        for _, new_site in site_df.iterrows():
            site_name = new_site['site_name']

            if not consolidated_sites.empty and site_name in consolidated_sites['site_name'].values:
                existing_idx = consolidated_sites[consolidated_sites['site_name'] == site_name].index[0]
                existing_site = consolidated_sites.loc[existing_idx]

                conflicts = detect_conflicts(site_name, existing_site, new_site)

                if conflicts:
                    conflict_record = {
                        'site_name': site_name,
                        'conflicts': conflicts,
                        'existing_source': existing_site['source_file'],
                        'new_source': new_site['source_file'],
                        'existing_data': existing_site.to_dict(),
                        'new_data': new_site.to_dict()
                    }
                    conflicts_list.append(conflict_record)
                    conflicts_found += 1
                else:
                    updated = False
                    for field in ['latitude', 'longitude', 'county', 'river_basin', 'ecoregion']:
                        if (pd.isna(existing_site[field]) and pd.notna(new_site[field])):
                            consolidated_sites.loc[existing_idx, field] = new_site[field]
                            consolidated_sites.loc[existing_idx, f'{field}_source'] = new_site['source_file']
                            updated = True

                    if updated:
                        sites_updated += 1
            else:
                new_record = new_site.copy()

                for field in ['latitude', 'longitude', 'county', 'river_basin', 'ecoregion']:
                    if pd.notna(new_site[field]):
                        new_record[f'{field}_source'] = new_site['source_file']
                    else:
                        new_record[f'{field}_source'] = None

                consolidated_sites = pd.concat([consolidated_sites, new_record.to_frame().T], ignore_index=True)
                sites_added += 1

        if sites_added > 0 or sites_updated > 0 or conflicts_found > 0:
            logger.info(f"  Added: {sites_added}, Updated: {sites_updated}, Conflicts: {conflicts_found}")

    # Process high-priority CSV sources (priorities 1-3)
    for i, config in enumerate(CSV_CONFIGS_HIGH):
        logger.info(f"\nProcessing priority {i+1}: {config['description']}")
        csv_sites = extract_sites_from_csv(config)
        if csv_sites.empty:
            logger.warning(f"No sites extracted from {config['file']}")
            continue
        _merge_sites(csv_sites, f"priority {i+1}")

    # Priority 4: Feature Server sites
    logger.info(f"\nProcessing priority 4: ArcGIS Feature Server")
    try:
        from data_processing.arcgis_sync import fetch_site_data
        fs_sites = fetch_site_data()
        if not fs_sites.empty:
            _merge_sites(fs_sites, "priority 4")
        else:
            logger.warning("No sites extracted from Feature Server")
    except Exception as e:
        logger.warning(f"Feature Server site fetch failed (non-fatal): {e}")

    # Process low-priority CSV sources (priorities 5-6)
    for i, config in enumerate(CSV_CONFIGS_LOW):
        priority = i + 5
        logger.info(f"\nProcessing priority {priority}: {config['description']}")
        csv_sites = extract_sites_from_csv(config)
        if csv_sites.empty:
            logger.warning(f"No sites extracted from {config['file']}")
            continue
        _merge_sites(csv_sites, f"priority {priority}")
    
    conflicts_df = pd.DataFrame(conflicts_list) if conflicts_list else pd.DataFrame()
    
    logger.info(f"\nConsolidation complete!")
    logger.info(f"Total consolidated sites: {len(consolidated_sites)}")
    logger.info(f"Total conflicts for review: {len(conflicts_df)}")
    
    return consolidated_sites, conflicts_df

def save_consolidated_data(consolidated_sites, conflicts_df):
    """
    Saves the consolidated site data and any conflicts to CSV files.
    
    Args:
        consolidated_sites: A DataFrame with the consolidated site data.
        conflicts_df: A DataFrame with any conflicts that require manual review.
        
    Returns:
        True if all files saved successfully, False otherwise.
    """
    try:
        # Save the master sites file, which is used for final site processing.
        master_path = os.path.join(PROCESSED_DATA_DIR, 'master_sites.csv')
        consolidated_sites.to_csv(master_path, index=False, encoding='utf-8')
        logger.info(f"Saved master sites to: master_sites.csv")
        
        # Save the full consolidated sites file, including source tracking, to the interim directory.
        consolidated_path = os.path.join(INTERIM_DATA_DIR, 'consolidated_sites.csv')
        consolidated_sites.to_csv(consolidated_path, index=False, encoding='utf-8')
        logger.info(f"Saved consolidated sites to: consolidated_sites.csv")
        
        if not conflicts_df.empty:
            conflicts_path = os.path.join(INTERIM_DATA_DIR, 'site_conflicts_for_review.csv')
            conflicts_df.to_csv(conflicts_path, index=False, encoding='utf-8')
            logger.warning(f"Saved {len(conflicts_df)} conflicts to: site_conflicts_for_review.csv")
            logger.warning("⚠️  Manual review required for conflicting sites!")
        else:
            logger.info("✓ No conflicts detected - all sites consolidated cleanly")
            
        return True
        
    except Exception as e:
        logger.error(f"Error saving consolidated data: {e}")
        return False

def verify_cleaned_csvs():
    """
    Verify that all required cleaned CSV files exist and are readable.
    
    Returns:
        True if all files are present and accessible, False otherwise.
    """
    logger.info("Verifying cleaned CSV files...")
    
    required_files = [config['file'] for config in CSV_CONFIGS]
    missing_files = []
    corrupted_files = []
    
    for filename in required_files:
        file_path = os.path.join(INTERIM_DATA_DIR, filename)
        
        if not os.path.exists(file_path):
            missing_files.append(filename)
            continue
            
        try:
            # Try to read the file to verify it's accessible
            df = pd.read_csv(file_path, nrows=1, low_memory=False)
        except Exception as e:
            logger.error(f"✗ {filename} is corrupted: {e}")
            corrupted_files.append(filename)
    
    if missing_files:
        logger.error(f"Missing cleaned CSV files: {missing_files}")
        logger.error("Run 'clean_all_csvs()' first to generate cleaned files")
        return False
        
    if corrupted_files:
        logger.error(f"Corrupted cleaned CSV files: {corrupted_files}")
        return False
    
    logger.info(f"All {len(required_files)} cleaned CSV files verified")
    return True

def consolidate_sites_from_csvs():
    """
    Consolidate sites from all cleaned CSV files into master sites list.
    
    This function calls the main consolidation process and ensures the
    master_sites.csv file is created properly.
    
    Returns:
        True if consolidation completed successfully, False otherwise.
    """
    logger.info("Consolidating sites from all cleaned CSV files...")
    
    try:
        # Verify files exist first
        if not verify_cleaned_csvs():
            return False
        
        # Run the main consolidation process
        consolidated_sites, conflicts_df = consolidate_sites()
        
        if consolidated_sites.empty:
            logger.error("Site consolidation produced no results")
            return False
        
        # Save the consolidated data
        save_success = save_consolidated_data(consolidated_sites, conflicts_df)
        
        if save_success:
            logger.info(f"Site consolidation completed: {len(consolidated_sites)} sites, {len(conflicts_df)} conflicts")
            return True
        else:
            logger.error("Failed to save consolidated site data")
            return False
            
    except Exception as e:
        logger.error(f"Error during site consolidation: {e}")
        return False

def main():
    """
    Executes the full site consolidation pipeline.
    """
    logger.info("=" * 60)
    logger.info("SITE CONSOLIDATION PIPELINE")
    logger.info("=" * 60)
    
    if not clean_all_csvs():
        logger.error("CSV cleaning failed. Check input files.")
        return False
    
    consolidated_sites, conflicts_df = consolidate_sites()
    
    if consolidated_sites.empty:
        logger.error("No sites were consolidated. Check input files.")
        return False
    
    save_consolidated_data(consolidated_sites, conflicts_df)
    
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info("✓ Phase 1: Cleaned all raw CSV files")
    logger.info("✓ Phase 2: Consolidated sites from cleaned CSVs")
    logger.info(f"✓ Final result: {len(consolidated_sites)} unique sites in master_sites.csv")
    
    if not conflicts_df.empty:
        logger.warning(f"⚠️  {len(conflicts_df)} conflicts need manual review")
        logger.warning("⚠️  See site_conflicts_for_review.csv")
    
    logger.info("\nOutput files:")
    logger.info("• Cleaned CSVs in data/interim/ (for other processors)")
    logger.info("• master_sites.csv in data/processed/ (for site processing)")
    
    logger.info("\nNext steps:")
    logger.info("1. Review any conflicts flagged above")
    logger.info("2. Other processors can use cleaned CSVs from interim/")
    logger.info("3. Site processor can use master_sites.csv")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)