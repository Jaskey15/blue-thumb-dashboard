"""
Tests for database schema creation and validation.

This module tests:
- Basic table creation and structure
- Column constraints and types
- Foreign key relationships
- Data integrity rules
- Reference data population
"""

import sqlite3

import pytest

from database.db_schema import create_tables, populate_chemical_reference_data


def verify_column_exists(cursor, table_name, column_name):
    """Helper function to verify column exists in table."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    return any(col[1] == column_name for col in columns)

def verify_foreign_key(cursor, table_name, column_name, ref_table, ref_column):
    """Helper function to verify foreign key constraint."""
    cursor.execute(f"PRAGMA foreign_key_list({table_name})")
    foreign_keys = cursor.fetchall()
    return any(
        fk[2] == ref_table and fk[3] == column_name and fk[4] == ref_column
        for fk in foreign_keys
    )

class TestBasicTableCreation:
    """Test basic table creation functionality."""
    
    def test_create_tables_success(self, temp_db):
        """Test that all tables are created successfully."""
        cursor = temp_db.cursor()
        
        # Check core tables exist
        core_tables = [
            'sites',
            'chemical_parameters',
            'chemical_reference_values',
            'chemical_collection_events',
            'chemical_measurements',
            'fish_collection_events',
            'fish_metrics',
            'fish_summary_scores',
            'macro_collection_events',
            'macro_metrics',
            'macro_summary_scores',
            'habitat_assessments'
        ]
        
        for table in core_tables:
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table,))
            assert cursor.fetchone() is not None, f"Table {table} was not created"
    
    def test_table_recreation_safety(self, temp_db):
        """Test that recreating tables doesn't lose data."""
        cursor = temp_db.cursor()
        
        # Insert test data
        cursor.execute("""
            INSERT INTO sites (site_name, latitude, longitude)
            VALUES ('Test Site', 35.0, -97.0)
        """)
        temp_db.commit()
        
        # Try recreating tables
        create_tables()
        
        # Verify data still exists
        cursor.execute("SELECT site_name FROM sites")
        result = cursor.fetchone()
        assert result is not None
        assert result[0] == 'Test Site'

class TestSitesSchema:
    """Test sites table schema and constraints."""
    
    def test_sites_columns(self, temp_db):
        """Test that sites table has correct columns."""
        cursor = temp_db.cursor()
        
        required_columns = {
            'site_id': 'INTEGER',
            'site_name': 'TEXT',
            'latitude': 'REAL',
            'longitude': 'REAL',
            'county': 'TEXT',
            'river_basin': 'TEXT',
            'ecoregion': 'TEXT',
            'active': 'BOOLEAN',
            'last_chemical_reading_date': 'TEXT'
        }
        
        cursor.execute("PRAGMA table_info(sites)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        for col_name, col_type in required_columns.items():
            assert col_name in columns, f"Column {col_name} missing from sites table"
            assert columns[col_name] == col_type, f"Column {col_name} has wrong type"
    
    def test_sites_constraints(self, temp_db):
        """Test sites table constraints."""
        cursor = temp_db.cursor()
        
        # Test site_name uniqueness
        cursor.execute("INSERT INTO sites (site_name) VALUES ('Test Site')")
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute("INSERT INTO sites (site_name) VALUES ('Test Site')")
        
        # Test site_id auto-increment
        cursor.execute("INSERT INTO sites (site_name) VALUES ('Test Site 2')")
        cursor.execute("SELECT site_id FROM sites ORDER BY site_id DESC LIMIT 1")
        last_id = cursor.fetchone()[0]
        assert last_id > 0, "site_id should auto-increment"

class TestChemicalSchema:
    """Test chemical data tables schema and relationships."""
    
    def test_chemical_parameters_structure(self, temp_db):
        """Test chemical_parameters table structure."""
        cursor = temp_db.cursor()
        
        # Verify columns
        required_columns = {
            'parameter_id': 'INTEGER',
            'parameter_name': 'TEXT',
            'parameter_code': 'TEXT',
            'display_name': 'TEXT',
            'unit': 'TEXT'
        }
        
        cursor.execute("PRAGMA table_info(chemical_parameters)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        for col_name, col_type in required_columns.items():
            assert col_name in columns
            assert columns[col_name] == col_type
    
    def test_chemical_reference_data(self, temp_db):
        """Test chemical reference data population."""
        cursor = temp_db.cursor()
        
        # Populate reference data
        populate_chemical_reference_data(cursor)
        
        # Verify parameters
        cursor.execute("SELECT COUNT(*) FROM chemical_parameters")
        param_count = cursor.fetchone()[0]
        assert param_count == 5, "Should have 5 chemical parameters"
        
        # Verify reference values
        cursor.execute("SELECT COUNT(*) FROM chemical_reference_values")
        ref_count = cursor.fetchone()[0]
        assert ref_count == 12, "Should have 12 reference values"
        
        # Verify relationships
        cursor.execute("""
            SELECT DISTINCT cp.parameter_name
            FROM chemical_parameters cp
            JOIN chemical_reference_values crv ON cp.parameter_id = crv.parameter_id
        """)
        params_with_refs = cursor.fetchall()
        assert len(params_with_refs) == 5, "All parameters should have reference values"

    def test_chemical_collection_events_sample_id_unique_index(self, temp_db):
        cursor = temp_db.cursor()

        cursor.execute("PRAGMA index_list(chemical_collection_events)")
        indexes = cursor.fetchall()
        names = {row[1] for row in indexes}
        assert 'idx_chemical_collection_events_sample_id' in names

        cursor.execute("PRAGMA index_info(idx_chemical_collection_events_sample_id)")
        cols = cursor.fetchall()
        col_names = [row[2] for row in cols]
        assert col_names == ['sample_id']
    
    def test_chemical_measurements_relationships(self, temp_db):
        """Test chemical measurements relationships."""
        cursor = temp_db.cursor()
        
        # Create test data
        cursor.execute("INSERT INTO sites (site_name) VALUES ('Test Site')")
        site_id = cursor.lastrowid
        
        cursor.execute("""
            INSERT INTO chemical_collection_events 
            (site_id, collection_date, year, month)
            VALUES (?, '2023-01-01', 2023, 1)
        """, (site_id,))
        event_id = cursor.lastrowid
        
        # Test foreign key constraint
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute("""
                INSERT INTO chemical_measurements 
                (event_id, parameter_id, value)
                VALUES (?, 999, 7.5)
            """, (event_id,))

class TestFishSchema:
    """Test fish data tables schema and relationships."""
    
    def test_fish_collection_events_structure(self, temp_db):
        """Test fish_collection_events table structure."""
        cursor = temp_db.cursor()
        
        # Verify unique constraint on site_id and sample_id
        cursor.execute("INSERT INTO sites (site_name) VALUES ('Fish Test Site')")
        site_id = cursor.lastrowid
        
        cursor.execute("""
            INSERT INTO fish_collection_events 
            (site_id, sample_id, collection_date, year)
            VALUES (?, 1, '2023-01-01', 2023)
        """, (site_id,))
        
        # Attempt to insert duplicate sample
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute("""
                INSERT INTO fish_collection_events 
                (site_id, sample_id, collection_date, year)
                VALUES (?, 1, '2023-02-01', 2023)
            """, (site_id,))
    
    def test_fish_metrics_relationships(self, temp_db):
        """Test fish metrics relationships."""
        cursor = temp_db.cursor()
        
        # Create test data
        cursor.execute("INSERT INTO sites (site_name) VALUES ('Fish Metrics Site')")
        site_id = cursor.lastrowid
        
        cursor.execute("""
            INSERT INTO fish_collection_events 
            (site_id, sample_id, collection_date, year)
            VALUES (?, 1, '2023-01-01', 2023)
        """, (site_id,))
        event_id = cursor.lastrowid
        
        # Test metrics insertion
        cursor.execute("""
            INSERT INTO fish_metrics 
            (event_id, metric_name, raw_value, metric_score)
            VALUES (?, 'Species Richness', 10, 5)
        """, (event_id,))
        
        # Test foreign key constraint
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute("""
                INSERT INTO fish_metrics 
                (event_id, metric_name, raw_value)
                VALUES (999, 'Invalid Event', 10)
            """)

class TestMacroSchema:
    """Test macroinvertebrate data tables schema and relationships."""
    
    def test_macro_collection_events_constraints(self, temp_db):
        """Test macro_collection_events constraints."""
        cursor = temp_db.cursor()
        
        # Create test site
        cursor.execute("INSERT INTO sites (site_name) VALUES ('Macro Test Site')")
        site_id = cursor.lastrowid
        
        # Test valid season values
        cursor.execute("""
            INSERT INTO macro_collection_events 
            (site_id, collection_date, season, year, habitat)
            VALUES (?, '2023-01-01', 'Winter', 2023, 'Riffle')
        """, (site_id,))
        
        # Test invalid season
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute("""
                INSERT INTO macro_collection_events 
                (site_id, collection_date, season, year, habitat)
                VALUES (?, '2023-06-01', 'Spring', 2023, 'Riffle')
            """, (site_id,))
        
        # Test valid habitat values
        cursor.execute("""
            INSERT INTO macro_collection_events 
            (site_id, collection_date, season, year, habitat)
            VALUES (?, '2023-01-02', 'Winter', 2023, 'Vegetation')
        """, (site_id,))
        
        # Test invalid habitat
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute("""
                INSERT INTO macro_collection_events 
                (site_id, collection_date, season, year, habitat)
                VALUES (?, '2023-01-03', 'Winter', 2023, 'Invalid')
            """, (site_id,))
    
    def test_macro_metrics_relationships(self, temp_db):
        """Test macro metrics relationships."""
        cursor = temp_db.cursor()
        
        # Create test data
        cursor.execute("INSERT INTO sites (site_name) VALUES ('Macro Metrics Site')")
        site_id = cursor.lastrowid
        
        cursor.execute("""
            INSERT INTO macro_collection_events 
            (site_id, collection_date, season, year, habitat)
            VALUES (?, '2023-01-01', 'Winter', 2023, 'Riffle')
        """, (site_id,))
        event_id = cursor.lastrowid
        
        # Test unique constraint on event_id and metric_name
        cursor.execute("""
            INSERT INTO macro_metrics 
            (event_id, metric_name, raw_value)
            VALUES (?, 'Taxa Richness', 15)
        """, (event_id,))
        
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute("""
                INSERT INTO macro_metrics 
                (event_id, metric_name, raw_value)
                VALUES (?, 'Taxa Richness', 16)
            """, (event_id,))

class TestHabitatSchema:
    """Test habitat assessment tables schema."""
    
    def test_habitat_assessments_structure(self, temp_db):
        """Test habitat_assessments table structure."""
        cursor = temp_db.cursor()
        
        # Create test site
        cursor.execute("INSERT INTO sites (site_name) VALUES ('Habitat Test Site')")
        site_id = cursor.lastrowid
        
        # Test basic insertion
        cursor.execute("""
            INSERT INTO habitat_assessments 
            (site_id, assessment_date, year)
            VALUES (?, '2023-01-01', 2023)
        """, (site_id,))
        
        # Test foreign key constraint
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute("""
                INSERT INTO habitat_assessments 
                (site_id, assessment_date, year)
                VALUES (999, '2023-01-01', 2023)
            """)

def test_schema_version_tracking(temp_db):
    """Test schema version tracking if implemented."""
    cursor = temp_db.cursor()
    
    # This test can be expanded when schema versioning is implemented
    # For now, just verify the basic structure works
    create_tables()
    
    # Verify we can still create and query tables
    cursor.execute("SELECT COUNT(*) FROM sites")
    assert cursor.fetchone() is not None 