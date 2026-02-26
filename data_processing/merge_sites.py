"""
Identifies and merges duplicate sites based on coordinate proximity.

This module analyzes sites with nearly identical coordinates and merges them,
preserving all associated monitoring data by transferring it to a single,
preferred site record.

The preferred site is determined using a priority system:
1. Sites present in the `updated_chemical_data` source file.
2. Sites present in the `chemical_data` source file.
3. The site with the longest name (as a fallback).
"""

import os

import math

import pandas as pd

from data_processing import setup_logging
from data_processing.data_loader import clean_site_name
from database.database import close_connection, get_connection

logger = setup_logging("merge_sites", category="processing")

def load_csv_files():
    """Loads cleaned source CSVs to check for site name existence."""
    base_dir = os.path.dirname(os.path.dirname(__file__))
    
    # Load cleaned CSVs from the interim directory for reference.
    site_data = pd.read_csv(os.path.join(base_dir, 'data', 'interim', 'cleaned_site_data.csv'))
    updated_chemical = pd.read_csv(os.path.join(base_dir, 'data', 'interim', 'cleaned_updated_chemical_data.csv'))
    chemical_data = pd.read_csv(os.path.join(base_dir, 'data', 'interim', 'cleaned_chemical_data.csv'))
    
    return site_data, updated_chemical, chemical_data

def find_duplicate_coordinate_groups(conn=None, distance_threshold_m=50.0):
    """Find candidate duplicate sites by Haversine distance clustering.

    Uses a two-stage approach:
      1) Candidate generation by binning coordinates into fixed floor bins:
         lat_bin = floor(latitude * 1000), lon_bin = floor(longitude * 1000).
         Bins correspond to ~0.001 degrees.
      2) For each site, compares against sites in the same bin and the 8 neighboring
         bins and computes Haversine distance. Pairs within distance_threshold_m are
         unioned into clusters via union-find (transitive).

    Args:
        conn: Optional SQLite connection. If omitted, opens/closes its own.
        distance_threshold_m: Distance threshold in meters for clustering (default 50.0).

    Returns:
        A pandas DataFrame of candidate duplicate sites with a group_id column
        identifying each cluster. Empty DataFrame when no duplicates detected.
    """
    if conn is None:
        conn = get_connection()
        should_close = True
    else:
        should_close = False

    try:
        query = """
        SELECT
            site_id,
            site_name,
            latitude,
            longitude,
            county,
            river_basin,
            ecoregion
        FROM sites
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY site_name
        """

        df = pd.read_sql_query(query, conn)
        df = df.reset_index(drop=True)

        scale = 1000
        bin_to_indices = {}
        lat_bins = [0] * len(df)
        lon_bins = [0] * len(df)
        for i, row in df.iterrows():
            lat_bin = math.floor(row['latitude'] * scale)
            lon_bin = math.floor(row['longitude'] * scale)
            lat_bins[i] = lat_bin
            lon_bins[i] = lon_bin
            bin_to_indices.setdefault((lat_bin, lon_bin), []).append(i)

        parent = list(range(len(df)))

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra = find(a)
            rb = find(b)
            if ra != rb:
                parent[rb] = ra

        def haversine_m(lat1, lon1, lat2, lon2):
            R = 6371000.0
            phi1, phi2 = math.radians(lat1), math.radians(lat2)
            dphi = math.radians(lat2 - lat1)
            dlambda = math.radians(lon2 - lon1)
            a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
            return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        for i, row in df.iterrows():
            base = (lat_bins[i], lon_bins[i])
            for dlat in (-1, 0, 1):
                for dlon in (-1, 0, 1):
                    nbr = (base[0] + dlat, base[1] + dlon)
                    for j in bin_to_indices.get(nbr, []):
                        if j <= i:
                            continue
                        dist = haversine_m(
                            row['latitude'],
                            row['longitude'],
                            df.at[j, 'latitude'],
                            df.at[j, 'longitude'],
                        )
                        if dist <= distance_threshold_m:
                            union(i, j)

        root_to_members = {}
        for i in range(len(df)):
            root = find(i)
            root_to_members.setdefault(root, []).append(i)

        dupe_groups = [members for members in root_to_members.values() if len(members) > 1]
        if not dupe_groups:
            empty = df.iloc[0:0].copy()
            empty['group_id'] = pd.Series(dtype='int64')
            return empty

        idx_to_group_id = {}
        out_indices = []
        group_id = 0
        for members in dupe_groups:
            for idx in members:
                idx_to_group_id[idx] = group_id
                out_indices.append(idx)
            group_id += 1

        result = df.loc[out_indices].copy()
        result['group_id'] = [idx_to_group_id[i] for i in result.index]
        result = result.sort_values(['group_id', 'site_name'])
        return result
    finally:
        if should_close:
            close_connection(conn)

def analyze_coordinate_duplicates(boundary_safe=True, distance_threshold_m=50.0, scale=1000):
    """Analyze duplicate groups without mutating the database.

    This is a read-only preview mode that:
    - Detects duplicate groups using `find_duplicate_coordinate_groups(...)`.
    - Applies `determine_preferred_site(...)` to each group to predict which site
      would be kept and why.

    Args:
        boundary_safe: If True, analyze distance-based clusters (see
            `find_duplicate_coordinate_groups`). If False, analyze strict
            3-decimal-rounded coordinate bins.
        distance_threshold_m: Distance threshold (meters) for boundary-safe clustering.
        scale: Bin scaling factor for boundary-safe candidate generation.

    Returns:
        A dictionary with summary statistics, including per-group site lists and
        a predicted keep decision.
        - In rounding mode, group identifiers are reported as `(rounded_lat, rounded_lon)`.
        - In boundary-safe mode, group identifiers are reported as `(group_id=<n>)`.
        Returns `None` on unexpected errors.
    """
    logger.info("Analyzing coordinate duplicates...")
    
    try:
        site_data_df, updated_chemical_df, chemical_data_df = load_csv_files()
        
        updated_chemical_sites = set(updated_chemical_df['Site Name'].apply(clean_site_name))
        chemical_data_sites = set(chemical_data_df['SiteName'].apply(clean_site_name))
        
        conn = get_connection()
        duplicate_groups_df = find_duplicate_coordinate_groups(
            conn,
            boundary_safe=boundary_safe,
            distance_threshold_m=distance_threshold_m,
            scale=scale,
        )
        close_connection(conn)
        
        if duplicate_groups_df.empty:
            logger.info("No coordinate duplicate sites found")
            return {
                'total_duplicate_sites': 0,
                'duplicate_groups': 0,
                'examples': []
            }
        
        duplicate_groups_summary = []
        total_duplicate_sites = len(duplicate_groups_df)
        group_count = 0

        groupby_cols = ['group_id'] if boundary_safe else ['rounded_lat', 'rounded_lon']
        
        # Process each group to determine which site would be kept.
        for group_key, group in duplicate_groups_df.groupby(groupby_cols):
            group_count += 1
            sites_in_group = list(group['site_name'])
            
            # Apply the same logic as the merge to predict the outcome.
            preferred_site, _, reason = determine_preferred_site(
                group, updated_chemical_sites, chemical_data_sites
            )

            if boundary_safe:
                coordinates = f"(group_id={group_key})"
            else:
                rounded_lat, rounded_lon = group_key
                coordinates = f"({rounded_lat}, {rounded_lon})"
            
            group_info = {
                'coordinates': coordinates,
                'site_count': len(group),
                'sites': sites_in_group,
                'would_keep': preferred_site['site_name'],
                'reason': reason
            }
            
            duplicate_groups_summary.append(group_info)
        
        logger.info(f"Found {total_duplicate_sites} duplicate sites in {group_count} coordinate groups")
        if total_duplicate_sites > group_count:
            logger.info(f"Would delete {total_duplicate_sites - group_count} duplicate sites")
        
        return {
            'total_duplicate_sites': total_duplicate_sites,
            'duplicate_groups': group_count,
            'examples': duplicate_groups_summary[:5],  # Provide first 5 for a sample.
            'all_groups': duplicate_groups_summary
        }
        
    except Exception as e:
        logger.error(f"Error analyzing coordinate duplicates: {e}")
        return None

def determine_preferred_site(group, updated_chemical_sites, chemical_data_sites):
    """
    Determines which site to keep from a group of duplicates.

    Returns:
        A tuple containing (preferred_site_row, sites_to_merge_list, reason).
    """
    if group.empty:
        return None, [], "Empty group"
    
    sites_in_updated = group[group['site_name'].isin(updated_chemical_sites)]
    sites_in_chemical = group[group['site_name'].isin(chemical_data_sites)]
    
    # Priority 1: Site exists in the `updated_chemical` source file.
    if len(sites_in_updated) > 1:
        # If multiple, prefer the one with the longest name.
        max_idx = sites_in_updated['site_name'].str.len().idxmax()
        preferred_site = sites_in_updated.loc[max_idx]
        sites_to_merge = group[group['site_id'] != preferred_site['site_id']].to_dict('records')
        return preferred_site, sites_to_merge, "Multiple in updated_chemical - keeping longer name"
    
    elif len(sites_in_updated) == 1:
        preferred_site = sites_in_updated.iloc[0]
        sites_to_merge = group[group['site_id'] != preferred_site['site_id']].to_dict('records')
        return preferred_site, sites_to_merge, "Found in updated_chemical"
    
    # Priority 2: Site exists in the `chemical_data` source file.
    elif len(sites_in_chemical) > 0:
        if len(sites_in_chemical) > 1:
            max_idx = sites_in_chemical['site_name'].str.len().idxmax()
            preferred_site = sites_in_chemical.loc[max_idx]
        else:
            preferred_site = sites_in_chemical.iloc[0]
        sites_to_merge = group[group['site_id'] != preferred_site['site_id']].to_dict('records')
        return preferred_site, sites_to_merge, "Found in chemical_data"
    
    # Fallback: No source file matches, so pick the longest name.
    else:
        max_idx = group['site_name'].str.len().idxmax()
        preferred_site = group.loc[max_idx]
        sites_to_merge = group[group['site_id'] != preferred_site['site_id']].to_dict('records')
        return preferred_site, sites_to_merge, "Arbitrary choice - longest name"

def transfer_site_data(cursor, from_site_id, to_site_id):
    """
    Transfers all monitoring data from a duplicate site to the preferred site.
    
    Returns: 
        A dictionary with counts of records transferred per table.
    """
    transfer_counts = {}
    
    # Convert numpy types to Python native types for SQLite compatibility
    from_site_id = int(from_site_id)
    to_site_id = int(to_site_id)
    
    # Verify both sites exist before attempting transfer
    cursor.execute("SELECT site_name FROM sites WHERE site_id = ?", (from_site_id,))
    from_site_result = cursor.fetchone()
    cursor.execute("SELECT site_name FROM sites WHERE site_id = ?", (to_site_id,))
    to_site_result = cursor.fetchone()
    
    if not from_site_result:
        logger.error(f"Source site_id {from_site_id} not found in database")
        raise Exception(f"Source site_id {from_site_id} not found")
    
    if not to_site_result:
        logger.error(f"Destination site_id {to_site_id} not found in database")
        raise Exception(f"Destination site_id {to_site_id} not found")
    
    tables_to_update = [
        ('chemical_collection_events', 'site_id'),
        ('fish_collection_events', 'site_id'),
        ('macro_collection_events', 'site_id'),
        ('habitat_assessments', 'site_id')
    ]
    
    for table_name, site_column in tables_to_update:
        try:
            # Check if there is data to transfer.
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {site_column} = ?", (from_site_id,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                # Perform the reassignment
                try:
                    cursor.execute(f"""
                        UPDATE {table_name} 
                        SET {site_column} = ? 
                        WHERE {site_column} = ?
                    """, (to_site_id, from_site_id))
                    
                    rows_affected = cursor.rowcount
                    
                    if rows_affected != count:
                        logger.warning(f"Expected to update {count} rows but updated {rows_affected} in {table_name}")
                    
                    transfer_counts[table_name] = rows_affected
                    
                except Exception as update_error:
                    logger.error(f"Failed to update {table_name}: {update_error}")
                    raise Exception(f"Failed to transfer data from {table_name}: {update_error}")
                    
            else:
                transfer_counts[table_name] = 0
                
        except Exception as e:
            logger.error(f"Error transferring data from {table_name}: {e}")
            raise
    
    return transfer_counts

def update_site_metadata(cursor, site_id, site_data_df, preferred_name):
    """Updates site metadata from site_data.csv if available."""
    # Convert numpy types to Python native types for SQLite compatibility
    site_id = int(site_id)
    
    metadata_row = site_data_df[site_data_df['SiteName'].str.strip() == preferred_name.strip()]
    
    if not metadata_row.empty:
        metadata = metadata_row.iloc[0]
        
        cursor.execute("""
            UPDATE sites 
            SET county = ?, river_basin = ?, ecoregion = ?
            WHERE site_id = ?
        """, (
            metadata.get('County'),
            metadata.get('RiverBasin'), 
            metadata.get('Mod_Ecoregion'),
            site_id
        ))
        
        return True
    else:
        return False

def update_csv_files_with_mapping(site_mapping):
    """
    Updates the cleaned CSV files to use preferred site names instead of deleted ones.
    
    Args:
        site_mapping: Dictionary mapping old site names to new site names
    """
    if not site_mapping:
        logger.info("No site mappings to apply to CSV files")
        return
    
    logger.info(f"Applying {len(site_mapping)} site name mappings to CSV files...")
    
    base_dir = os.path.dirname(os.path.dirname(__file__))
    interim_dir = os.path.join(base_dir, 'data', 'interim')
    
    # Define CSV files and their site name columns
    csv_configs = [
        {'file': 'cleaned_chemical_data.csv', 'site_column': 'SiteName'},
        {'file': 'cleaned_updated_chemical_data.csv', 'site_column': 'Site Name'},
        {'file': 'cleaned_fish_data.csv', 'site_column': 'SiteName'},
        {'file': 'cleaned_macro_data.csv', 'site_column': 'SiteName'},
        {'file': 'cleaned_habitat_data.csv', 'site_column': 'SiteName'},
    ]
    
    total_updates = 0
    files_updated = 0
    
    for config in csv_configs:
        file_path = os.path.join(interim_dir, config['file'])
        
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {config['file']}")
            continue
            
        try:
            # Load CSV
            df = pd.read_csv(file_path)
            
            if config['site_column'] not in df.columns:
                logger.warning(f"Site column '{config['site_column']}' not found in {config['file']}")
                continue
            
            # Apply site name mappings
            updates_in_file = 0
            
            for old_name, new_name in site_mapping.items():
                mask = df[config['site_column']] == old_name
                update_count = mask.sum()
                
                if update_count > 0:
                    df.loc[mask, config['site_column']] = new_name
                    updates_in_file += update_count
            
            # Save updated CSV if changes were made
            if updates_in_file > 0:
                df.to_csv(file_path, index=False)
                logger.info(f"Updated {config['file']}: {updates_in_file} records redirected")
                total_updates += updates_in_file
                files_updated += 1
                
        except Exception as e:
            logger.error(f"Error updating {config['file']}: {e}")
    
    if total_updates > 0:
        logger.info(f"CSV mapping complete: {total_updates} records updated across {files_updated} files")
    else:
        logger.info("No CSV updates needed - site names already current")

def merge_duplicate_sites(boundary_safe=True, distance_threshold_m=50.0, scale=1000):
    """Merge duplicate sites by transferring monitoring data and deleting extras.

    This function mutates the SQLite database by:
    - Selecting a preferred site for each duplicate group via `determine_preferred_site(...)`.
    - Reassigning all monitoring data from duplicate site(s) to the preferred site
      via `transfer_site_data(...)`.
    - Deleting the now-empty duplicate site rows from `sites`.
    - (Optionally) updating cleaned interim CSVs to replace deleted site names.

    Grouping behavior:
    - `boundary_safe=False` (default): groups are formed strictly by identical
      `ROUND(latitude, 3)` and `ROUND(longitude, 3)` bins.
    - `boundary_safe=True`: groups are formed by distance-based clustering using
      neighbor-bin expansion + Haversine threshold (see `find_duplicate_coordinate_groups`).
      Clustering is transitive, so chain-connected sites can be merged into a single group.

    Args:
        boundary_safe: If True, merge distance-based clusters; otherwise merge
            strict 3-decimal-rounded coordinate bins.
        distance_threshold_m: Distance threshold (meters) for boundary-safe clustering.
        scale: Bin scaling factor for boundary-safe candidate generation.

    Returns:
        A dictionary containing counts of processed groups, deleted sites, and
        transferred records.
    """
    logger.info("Starting coordinate-based site merge process...")
    
    try:
        site_data_df, updated_chemical_df, chemical_data_df = load_csv_files()
        
        updated_chemical_sites = set(updated_chemical_df['Site Name'].apply(clean_site_name))
        chemical_data_sites = set(chemical_data_df['SiteName'].apply(clean_site_name))
        
        conn = get_connection()
        cursor = conn.cursor()
        
        duplicate_groups_df = find_duplicate_coordinate_groups(
            conn,
            boundary_safe=boundary_safe,
            distance_threshold_m=distance_threshold_m,
            scale=scale,
        )

        groupby_cols = ['group_id'] if boundary_safe else ['rounded_lat', 'rounded_lon']
        
        groups_processed = 0
        sites_deleted = 0
        total_records_transferred = 0
        site_mapping = {}  # Track mapping from deleted sites to preferred sites
        
        try:
            if not duplicate_groups_df.empty:
                logger.info(f"Found {len(duplicate_groups_df.groupby(groupby_cols))} coordinate groups with duplicates")
                
                for _, group in duplicate_groups_df.groupby(groupby_cols):
                    preferred_site, sites_to_merge, reason = determine_preferred_site(
                        group, updated_chemical_sites, chemical_data_sites
                    )
                    
                    if not preferred_site is None and sites_to_merge:
                        # Verify preferred site exists before proceeding  
                        # Convert numpy types to Python native types for SQLite compatibility
                        preferred_site_id = int(preferred_site['site_id'])
                        
                        cursor.execute("SELECT site_name FROM sites WHERE site_id = ?", (preferred_site_id,))
                        preferred_site_check = cursor.fetchone()
                        
                        if not preferred_site_check:
                            logger.error(f"CRITICAL: Preferred site_id {preferred_site_id} ('{preferred_site['site_name']}') not found in database!")
                            raise Exception(f"Preferred site_id {preferred_site_id} not found in database")
                        
                        # Process all sites to merge in this group
                        for site_to_merge in sites_to_merge:
                            from_site_id = int(site_to_merge['site_id'])
                            
                            transfer_counts = transfer_site_data(cursor, from_site_id, preferred_site_id)
                            total_records_transferred += sum(transfer_counts.values())
                            
                            cursor.execute("DELETE FROM sites WHERE site_id = ?", (from_site_id,))
                            sites_deleted += 1
                            
                            # Add to site mapping
                            old_site_name = site_to_merge['site_name']
                            new_site_name = preferred_site['site_name']
                            site_mapping[old_site_name] = new_site_name
                        
                        update_site_metadata(cursor, preferred_site_id, site_data_df, preferred_site['site_name'])
                        
                        groups_processed += 1
            
            conn.commit()
            logger.info(f"Site merge complete: {groups_processed} groups processed, {sites_deleted} sites deleted, {total_records_transferred} records transferred")
            
            # Apply site name mapping to CSV files
            if site_mapping:
                update_csv_files_with_mapping(site_mapping)
            
            return {
                'groups_processed': groups_processed,
                'sites_deleted': sites_deleted,
                'records_transferred': total_records_transferred
            }
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during site merge: {e}")
            raise
            
    except Exception as e:
        logger.error(f"Error in coordinate merge process: {e}")
        raise
    finally:
        if 'conn' in locals():
            close_connection(conn)

if __name__ == "__main__":
    # When run directly, analyze duplicates without merging.
    print("🔍 Analyzing coordinate duplicates...")
    
    analysis = analyze_coordinate_duplicates()
    
    if analysis:
        print(f"\n📊 ANALYSIS RESULTS:")
        print(f"Total duplicate sites: {analysis['total_duplicate_sites']}")
        print(f"Duplicate groups: {analysis['duplicate_groups']}")
        print(f"Sites that would be deleted: {analysis['total_duplicate_sites'] - analysis['duplicate_groups']}")
        
        if analysis['duplicate_groups'] > 0:
            print(f"\n📝 Sample duplicate groups:")
            for i, group in enumerate(analysis['examples'], 1):
                print(f"{i}. {group['coordinates']}: {group['sites']} → Keep: {group['would_keep']}")
        
        print("\nTo execute the merge, call merge_duplicate_sites() function")
    else:
        print("❌ Analysis failed. Check logs for details.")