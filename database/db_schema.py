"""
Database schema and initialization for the Blue Thumb Water Quality Dashboard.
"""

from database.database import get_connection, close_connection
from utils import setup_logging

# Set up logging
logger = setup_logging("db_schema", category="database")

# ---------- CHEMICAL DATA CONSTANTS ----------
CHEMICAL_PARAMETERS = [
    (1, 'Dissolved Oxygen', 'do_percent', 'Dissolved Oxygen', '%'),
    (2, 'pH', 'pH', 'pH', 'pH units'),
    (3, 'Soluble Nitrogen', 'soluble_nitrogen', 'Nitrogen', 'mg/L'),
    (4, 'Phosphorus', 'Phosphorus', 'Phosphorus', 'mg/L'),
    (5, 'Chloride', 'Chloride', 'Chloride', 'mg/L')
]

CHEMICAL_REFERENCE_VALUES = [
    # do_percent reference values
    (1, 1, 'normal_min', 80),
    (2, 1, 'normal_max', 130),
    (3, 1, 'caution_min', 50),
    (4, 1, 'caution_max', 150),
    
    # pH reference values
    (5, 2, 'normal_min', 6.5),
    (6, 2, 'normal_max', 9.0),
    
    # Soluble Nitrogen reference values
    (7, 3, 'normal', 0.8),
    (8, 3, 'caution', 1.5),
    
    # Phosphorus reference values
    (9, 4, 'normal', 0.05),
    (10, 4, 'caution', 0.1),
    
    # Chloride reference values
    (11, 5, 'normal', 200),
    (12, 5, 'caution', 400)
]

def populate_chemical_reference_data(cursor):
    """
    Populate chemical parameters and reference values after table creation.
    This ensures reference data exists before any processing occurs.
    
    Args:
        cursor: Database cursor
    """
    try:
        # Insert the parameters
        cursor.executemany('''
        INSERT OR IGNORE INTO chemical_parameters 
        (parameter_id, parameter_name, parameter_code, display_name, unit)
        VALUES (?, ?, ?, ?, ?)
        ''', CHEMICAL_PARAMETERS)
        
        # Insert the reference values
        cursor.executemany('''
        INSERT OR IGNORE INTO chemical_reference_values
        (reference_id, parameter_id, threshold_type, value)
        VALUES (?, ?, ?, ?)
        ''', CHEMICAL_REFERENCE_VALUES)
        
        logger.info(f"Successfully populated chemical reference data")
        logger.info(f"  - {len(CHEMICAL_PARAMETERS)} parameters")
        logger.info(f"  - {len(CHEMICAL_REFERENCE_VALUES)} reference values")
        
    except Exception as e:
        logger.error(f"Error populating chemical reference data: {e}")
        raise Exception(f"Failed to populate chemical reference data: {e}")

def create_tables():
    """Create all database tables if they don't exist."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Sites table - common to all data types
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sites (
        site_id INTEGER PRIMARY KEY,
        site_name TEXT NOT NULL,
        latitude REAL,
        longitude REAL,
        county TEXT,
        river_basin TEXT,
        ecoregion TEXT,
        active BOOLEAN DEFAULT 1,
        last_chemical_reading_date TEXT,
        UNIQUE(site_name)
    )
    ''')

    # ---------- CHEMICAL DATA TABLES ----------
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chemical_parameters (
        parameter_id INTEGER PRIMARY KEY,
        parameter_name TEXT NOT NULL,
        parameter_code TEXT NOT NULL,
        display_name TEXT NOT NULL,
        unit TEXT,
        UNIQUE(parameter_code)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chemical_reference_values (
        reference_id INTEGER PRIMARY KEY,
        parameter_id INTEGER NOT NULL,
        threshold_type TEXT NOT NULL,
        value REAL NOT NULL,
        FOREIGN KEY (parameter_id) REFERENCES chemical_parameters (parameter_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chemical_collection_events (
        event_id INTEGER PRIMARY KEY,
        site_id INTEGER NOT NULL,
        sample_id INTEGER,
        collection_date TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        FOREIGN KEY (site_id) REFERENCES sites (site_id)
    )
    ''')

    cursor.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_chemical_collection_events_sample_id
        ON chemical_collection_events(sample_id)
        WHERE sample_id IS NOT NULL
        """
    )
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS chemical_measurements (
        event_id INTEGER NOT NULL,
        parameter_id INTEGER NOT NULL,
        value REAL,
        bdl_flag BOOLEAN DEFAULT 0,
        status TEXT,
        PRIMARY KEY (event_id, parameter_id),
        FOREIGN KEY (event_id) REFERENCES chemical_collection_events (event_id),
        FOREIGN KEY (parameter_id) REFERENCES chemical_parameters (parameter_id)
    )
    ''')

    # ---------- FISH DATA TABLES ----------
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fish_collection_events (
        event_id INTEGER PRIMARY KEY,
        site_id INTEGER NOT NULL,
        sample_id INTEGER NOT NULL,
        collection_date TEXT NOT NULL,
        year INTEGER NOT NULL,
        FOREIGN KEY (site_id) REFERENCES sites (site_id),
        UNIQUE(site_id, sample_id)
    )
    ''')

    # Reference values table - currently don't have access to this data
    # If reference values by region become available in the future, uncomment this table
    '''
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fish_reference_values (
        reference_id INTEGER PRIMARY KEY,
        region TEXT NOT NULL,
        metric_name TEXT NOT NULL,
        metric_value REAL,
        UNIQUE (region, metric_name)
    )
    """)
    '''  

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fish_metrics (
        event_id INTEGER NOT NULL,
        metric_name TEXT NOT NULL,
        raw_value REAL NOT NULL,
        metric_result REAL,
        metric_score INTEGER,
        PRIMARY KEY (event_id, metric_name),
        FOREIGN KEY (event_id) REFERENCES fish_collection_events (event_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fish_summary_scores (
        event_id INTEGER NOT NULL,
        total_score INTEGER,
        comparison_to_reference REAL NOT NULL,
        integrity_class TEXT NOT NULL,
        FOREIGN KEY (event_id) REFERENCES fish_collection_events (event_id)
    )
    ''')
    
    # ---------- MACROINVERTEBRATE DATA TABLES ----------
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS macro_collection_events (
        event_id INTEGER PRIMARY KEY,
        site_id INTEGER NOT NULL,
        sample_id INTEGER,
        collection_date TEXT NOT NULL,  
        season TEXT CHECK (season IN ('Summer', 'Winter')),
        year INTEGER NOT NULL,
        habitat TEXT CHECK (habitat IN ('Riffle', 'Vegetation', 'Woody')),
        FOREIGN KEY (site_id) REFERENCES sites (site_id)
        UNIQUE(site_id, sample_id, habitat)
    )
    ''')

    # Reference values table - currently don't have access to this data
    # If reference values by region become available in the future, uncomment this table
    '''
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS macro_reference_values (
        reference_id INTEGER PRIMARY KEY,
        region TEXT NOT NULL,
        season TEXT CHECK (season IN ('Summer', 'Winter')),
        habitat TEXT CHECK (habitat IN ('Riffle', 'Vegetation', 'Woody')),
        metric_name TEXT NOT NULL,
        metric_value REAL, 
        UNIQUE (region, season, habitat, metric_name)
    )
    """)  
    '''

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS macro_metrics (
        metric_id INTEGER PRIMARY KEY,  
        event_id INTEGER NOT NULL,
        metric_name TEXT NOT NULL,
        raw_value REAL NOT NULL,
        metric_score INTEGER,
        FOREIGN KEY (event_id) REFERENCES macro_collection_events (event_id),
        UNIQUE (event_id, metric_name)  
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS macro_summary_scores (
        event_id INTEGER NOT NULL,
        total_score INTEGER,
        comparison_to_reference REAL NOT NULL,
        biological_condition TEXT NOT NULL,
        FOREIGN KEY (event_id) REFERENCES macro_collection_events (event_id)
    )
    ''')
    
    # ---------- HABITAT DATA TABLES ----------
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS habitat_assessments (
        assessment_id INTEGER PRIMARY KEY,
        site_id INTEGER NOT NULL,
        assessment_date TEXT NOT NULL,
        year INTEGER NOT NULL,
        FOREIGN KEY (site_id) REFERENCES sites (site_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS habitat_metrics (
        assessment_id INTEGER NOT NULL,
        metric_name TEXT NOT NULL,
        score REAL NOT NULL,
        PRIMARY KEY (assessment_id, metric_name),
        FOREIGN KEY (assessment_id) REFERENCES habitat_assessments (assessment_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS habitat_summary_scores (
    assessment_id INTEGER NOT NULL,
    total_score REAL NOT NULL,
    habitat_grade TEXT NOT NULL,
    FOREIGN KEY (assessment_id) REFERENCES habitat_assessments (assessment_id)
    )
    ''')

    # Create database indexes to optimize map queries by site, date, and season
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_chemical_site_date ON chemical_collection_events(site_id, collection_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_chemical_measurements ON chemical_measurements(event_id, parameter_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_macro_site_season ON macro_collection_events(site_id, season, year)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fish_site_year ON fish_collection_events(site_id, year)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_habitat_site_year ON habitat_assessments(site_id, year)')
    
    # Populate chemical reference data
    populate_chemical_reference_data(cursor)
    
    # Commit all changes
    conn.commit()
    logger.info("Database schema created successfully")
    
    # Close connection
    close_connection(conn)

if __name__ == "__main__":
    create_tables()