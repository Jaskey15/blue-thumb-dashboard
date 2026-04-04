"""
reset_database.py - Utility to reset the database and reload all data.
Use this script to quickly rebuild your database after schema changes.
"""

import os
import time
import traceback
from database.database import get_connection, close_connection
from database.db_schema import create_tables
from data_processing.consolidate_sites import verify_cleaned_csvs, consolidate_sites_from_csvs
from data_processing.site_processing import process_site_data, classify_active_sites, cleanup_unused_sites
from data_processing.merge_sites import merge_duplicate_sites
from data_processing.chemical_processing import load_chemical_data_to_db
from data_processing.arcgis_sync import sync_all_chemical_data
from data_processing.fish_processing import load_fish_data
from data_processing.macro_processing import load_macroinvertebrate_data
from data_processing.habitat_processing import load_habitat_data
from utils import setup_logging

logger = setup_logging("reset_database", category="database")

def generate_final_data_summary():
    """Generate comprehensive summary of all data in the database."""
    conn = get_connection()
    try:
        # Sites summary
        sites_total = conn.execute("SELECT COUNT(*) FROM sites").fetchone()[0]
        sites_active = conn.execute("SELECT COUNT(*) FROM sites WHERE active = 1").fetchone()[0]
        sites_historic = sites_total - sites_active
        
        # Chemical data summary
        chemical_events = conn.execute("SELECT COUNT(*) FROM chemical_collection_events").fetchone()[0]
        chemical_measurements = conn.execute("SELECT COUNT(*) FROM chemical_measurements").fetchone()[0]
        
        # Biological data summary
        fish_events = conn.execute("SELECT COUNT(*) FROM fish_collection_events").fetchone()[0]
        macro_events = conn.execute("SELECT COUNT(*) FROM macro_collection_events").fetchone()[0]
        
        # Habitat data summary
        habitat_assessments = conn.execute("SELECT COUNT(*) FROM habitat_assessments").fetchone()[0]
        
        return {
            'sites': {
                'total': sites_total,
                'active': sites_active,
                'historic': sites_historic
            },
            'chemical': {
                'events': chemical_events,
                'measurements': chemical_measurements
            },
            'biological': {
                'fish_events': fish_events,
                'macro_events': macro_events
            },
            'habitat': {
                'assessments': habitat_assessments
            }
        }
    finally:
        close_connection(conn)

def delete_database_file():
    """Delete the SQLite database file if it exists."""
    try:
        # Determine database path based on database.py module
        conn = get_connection()
        db_path = conn.execute("PRAGMA database_list").fetchone()[2]  # Get the path from SQLite
        conn.close()
        
        # Delete the file
        if os.path.exists(db_path):
            os.remove(db_path)
            logger.info(f"Deleted database file: {db_path}")
            return True
        else:
            logger.info("No database file found to delete")
            return True
    except Exception as e:
        logger.error(f"Error deleting database file: {e}")
        return False

def recreate_schema():
    """Recreate the database schema."""
    try:
        create_tables()
        logger.info("Recreated database schema")
        return True
    except Exception as e:
        logger.error(f"Error recreating database schema: {e}")
        return False

def reload_all_data():
    """
    Reload all data using the 'Sites First' approach.
    
    This approach ensures maximum data integrity by:
    1. First consolidating and merging ALL sites from all data sources
    2. Then loading monitoring data against the unified site list
    3. Finally performing cleanup and classification
    
    Returns:
        True if all steps complete successfully, False otherwise
    """
    try:        
        start_time = time.time()
        
        logger.info("="*80)
        logger.info("STARTING 'SITES FIRST' DATA RELOAD PIPELINE")
        logger.info("="*80)
        
        # PHASE 1: COMPLETE SITE UNIFICATION (BEFORE ANY MONITORING DATA)
        
        logger.info("\n" + "="*60)
        logger.info("PHASE 1: COMPLETE SITE UNIFICATION")
        logger.info("="*60)
        
        # Step 1: Ensure all CSVs are cleaned and ready
        csv_status = verify_cleaned_csvs()
        if not csv_status:
            logger.error("CSV cleaning verification failed. Run consolidate_sites.py first.")
            return False
        
        # Step 2: Consolidate master sites list from all CSV sources
        consolidate_result = consolidate_sites_from_csvs()
        if not consolidate_result:
            logger.error("Site consolidation failed. Cannot continue.")
            return False
        
        # Step 3: Load master sites into database
        site_success = process_site_data()
        if not site_success:
            logger.error("Site processing failed. Cannot continue with data processing.")
            return False
        
        # Step 4: Merge coordinate duplicates (BEFORE loading any monitoring data)
        merge_result = merge_duplicate_sites()
        if not merge_result:
            logger.warning("Site merging had issues, but continuing...")
        
        # Step 5: Generate site summary for monitoring data loading
        final_summary = generate_final_data_summary()
        logger.info(f"Site unification complete: {final_summary['sites']['total']} sites")
        
        # PHASE 2: LOAD MONITORING DATA 
        
        logger.info("\n" + "="*60)
        logger.info("PHASE 2: MONITORING DATA LOADING")
        logger.info("="*60)
        
        # Step 6: Load chemical data
        try:
            chemical_result = load_chemical_data_to_db()
            if chemical_result:
                logger.info("Chemical data loaded successfully")
            else:
                logger.warning("Chemical data loading had issues, but continuing...")
        except Exception as e:
            logger.error(f"Chemical data loading failed: {e}")
            return False
        
        # Step 7: Load current-period chemical data from Feature Server
        try:
            updated_result = sync_all_chemical_data()
            if updated_result.get('status') == 'success':
                logger.info(
                    f"Feature Server chemical data loaded: "
                    f"{updated_result.get('records_inserted', 0)} measurements inserted"
                )
            else:
                logger.warning("Feature Server chemical data loading had issues, but continuing...")
        except Exception as e:
            logger.error(f"Feature Server chemical data loading failed: {e}")
            return False
        
        # Step 8: Load fish data
        try:
            fish_result = load_fish_data()
            if fish_result:
                logger.info("Fish data loaded successfully")
            else:
                logger.warning("Fish data loading had issues, but continuing...")
        except Exception as e:
            logger.warning(f"Fish data loading had issues: {e}")
        
        # Step 9: Load macroinvertebrate data
        try:
            macro_result = load_macroinvertebrate_data()
            if macro_result is not None and not (hasattr(macro_result, 'empty') and macro_result.empty):
                logger.info("Macro data loaded successfully")
            else:
                logger.warning("Macro data loading had issues, but continuing...")
        except Exception as e:
            logger.warning(f"Macro data loading had issues: {e}")

        # Step 10: Load habitat data
        try:
            habitat_result = load_habitat_data()
            if habitat_result is not None and not (hasattr(habitat_result, 'empty') and habitat_result.empty):
                logger.info("Habitat data loaded successfully")
            else:
                logger.warning("Habitat data loading had issues, but continuing...")
        except Exception as e:
            logger.warning(f"Habitat data loading had issues: {e}")

        # PHASE 3: FINAL DATA QUALITY AND CLEANUP
        
        logger.info("\n" + "="*60)
        logger.info("PHASE 3: FINAL DATA QUALITY AND CLEANUP")
        logger.info("="*60)
        
        # Step 11: Final site classification with all data loaded
        final_classification_result = classify_active_sites()
        if not final_classification_result:
            logger.warning("Final site classification had issues, but continuing...")
        
        # Step 12: Cleanup unused sites
        cleanup_result = cleanup_unused_sites()
        if not cleanup_result:
            logger.warning("Site cleanup had issues, but continuing...")
        
        # Step 13: Generate final data summary
        final_summary = generate_final_data_summary()
        
        elapsed_time = time.time() - start_time
        
        # FINAL RESULTS
        
        logger.info("\n" + "="*80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info(f"Total execution time: {elapsed_time:.2f} seconds")
        logger.info("\nFinal Data Summary:")
        logger.info(f"  Sites:")
        logger.info(f"    - Total sites: {final_summary['sites']['total']}")
        logger.info(f"    - Active sites: {final_summary['sites']['active']}")
        logger.info(f"    - Historic sites: {final_summary['sites']['historic']}")
        logger.info(f"  Chemical Data:")
        logger.info(f"    - Collection events: {final_summary['chemical']['events']}")
        logger.info(f"    - Measurements: {final_summary['chemical']['measurements']}")
        logger.info(f"  Biological Data:")
        logger.info(f"    - Fish events: {final_summary['biological']['fish_events']}")
        logger.info(f"    - Macro events: {final_summary['biological']['macro_events']}")
        logger.info(f"  Habitat Data:")
        logger.info(f"    - Assessments: {final_summary['habitat']['assessments']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in data reload pipeline: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return False

def reset_database():
    """Perform complete database reset and reload."""
    logger.info("Starting database reset process...")

    if not delete_database_file():
        logger.error("Database deletion failed. Aborting reset.")
        return False
    
    if not recreate_schema():
        logger.error("Schema recreation failed. Aborting reset.")
        return False
    
    if not reload_all_data():
        logger.error("Data reloading failed. Reset process incomplete.")
        return False
    
    logger.info("Database reset process completed successfully!")
    return True

if __name__ == "__main__":
    success = reset_database()
    if success:
        print("Database has been successfully reset and all data reloaded.")
    else:
        print("Database reset failed. Check the logs for details.")