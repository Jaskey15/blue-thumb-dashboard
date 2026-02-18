# Site Processing Pipeline

## Overview
The site processing pipeline implements a **"Sites First"** approach that manages the complete workflow from raw CSV files with inconsistent site data to a clean, consolidated database of unique monitoring sites. This three-phase process handles site name standardization, duplicate detection, metadata consolidation, and database management.

The pipeline ensures that all sites are properly established in the database before any monitoring data is loaded, eliminating foreign key constraint issues and enabling reliable data processing.

## Complete Workflow

### Phase 1: Site Consolidation (`consolidate_sites.py`)
**Raw CSV files → Cleaned CSVs → Master Sites List**

### Phase 2: Coordinate Duplicate Merging (`merge_sites.py`)  
**Database sites → Coordinate analysis → Merged duplicates**

### Phase 3: Database Site Management (`site_processing.py`)
**Master sites → Database loading → Site classification**

---

## Sites First Approach

### Core Principle
**All sites must exist in the database before any monitoring data is loaded.** This approach:

1. **Prevents Foreign Key Errors**: Sites are guaranteed to exist when referenced by monitoring data
2. **Enables Data Validation**: Site matching can be validated before data insertion
3. **Improves Reliability**: Pipeline failures are caught early in the process
4. **Simplifies Debugging**: Clear separation between site management and data loading

### Pipeline Order
1. **Site Processing**: Complete all three phases of site consolidation and management
2. **Data Loading**: Load chemical, biological, and habitat monitoring data
3. **Final Classification**: Update site active/historic status based on loaded data

---

## Phase 1: Site Consolidation

### Purpose
Consolidate sites from multiple raw CSV files into a single master sites list with the best available metadata from priority sources.

### Two-Step Process

#### Step 1: CSV Cleaning
- **Input**: Raw CSV files with inconsistent site names
- **Process**: Standardize site names by stripping whitespace and normalizing spaces
- **Output**: Cleaned CSV files in `data/interim/` directory

**Files Processed:**
- `site_data.csv` → `cleaned_site_data.csv`
- `chemical_data.csv` → `cleaned_chemical_data.csv`
- `updated_chemical_data.csv` → `cleaned_updated_chemical_data.csv`
- `fish_data.csv` → `cleaned_fish_data.csv`
- `macro_data.csv` → `cleaned_macro_data.csv`
- `habitat_data.csv` → `cleaned_habitat_data.csv`

#### Step 2: Site Consolidation
- **Input**: Cleaned CSV files
- **Process**: Extract unique sites and consolidate metadata using priority system
- **Output**: `master_sites.csv` with consolidated site information

### Priority-Based Metadata Resolution

**Priority Order (Highest to Lowest):**
1. **Master site data** (`cleaned_site_data.csv`) - Most comprehensive metadata
2. **Original chemical data** (`cleaned_chemical_data.csv`) - Well-established sites
3. **Fish community data** (`cleaned_fish_data.csv`) - Biological monitoring sites
4. **Updated chemical data** (`cleaned_updated_chemical_data.csv`) - Recent additions
5. **Macroinvertebrate data** (`cleaned_macro_data.csv`) - Specialized monitoring
6. **Habitat assessment data** (`cleaned_habitat_data.csv`) - Limited metadata

### Metadata Fields Tracked
- **Coordinates**: `latitude`, `longitude`
- **Geographic**: `county`, `river_basin`, `ecoregion`
- **Source Tracking**: Records which file provided each metadata field

### Conflict Detection
**Conflicts occur when:**
- Same site name has different values for the same metadata field
- Both values are non-null and different

**Conflict Resolution:**
- Conflicts are flagged for manual review in `site_conflicts_for_review.csv`
- Higher priority sources take precedence for non-conflicting metadata
- Missing metadata is filled from lower priority sources

---

## Phase 2: Coordinate Duplicate Merging

### Purpose
Identify and merge sites that represent the same physical location but have different names in the database.

### Duplicate Detection Logic
- **Method**: Round coordinates to 3 decimal places (~111 meters precision)
- **Grouping**: Sites with identical rounded coordinates are considered duplicates
- **Analysis**: Preview mode shows what would be merged without making changes

### Site Selection Priority

**When multiple sites share coordinates, keep the site with highest priority:**

1. **Sites in updated_chemical_data** (highest priority)
   - Most recent and actively monitored locations
2. **Sites in chemical_data**
   - Established monitoring locations
3. **Longest site name** (fallback)
   - Assumption that longer names are more descriptive

### Data Transfer Process

**All monitoring data is transferred from duplicate sites to the preferred site:**
- `chemical_collection_events`
- `fish_collection_events`
- `macro_collection_events`
- `habitat_assessments`

**Process:**
1. Update all foreign key references to point to preferred site
2. Delete duplicate site records
3. Preserve all historical monitoring data

### Execution Modes

#### Analysis Mode (Default)
```python
analysis = analyze_coordinate_duplicates()
# Preview what would be merged without making changes
```

#### Execution Mode
```python
success = merge_duplicate_sites()
# Execute the actual merge process
```

---

## Phase 3: Database Site Management

### Purpose
Load the master sites list into the database and manage site lifecycle (active/historic classification, cleanup).

### Master Site Loading
- **Input**: `master_sites.csv` from Phase 1
- **Process**: Insert/update sites in database with proper schema alignment
- **Database Schema**: Only includes fields that match the sites table structure

**Schema Alignment:**
```python
database_columns = ['site_name', 'latitude', 'longitude', 'county', 'river_basin', 'ecoregion']
```

### Active vs Historic Site Classification

**Classification Logic:**
- **Active Site**: Has chemical readings within 1 year of the most recent reading date across all sites
- **Historic Site**: No recent chemical data or readings older than 1-year cutoff

**Process:**
1. Find most recent chemical reading date across all sites
2. Calculate cutoff date (1 year before most recent reading)
3. Classify each site based on its most recent chemical reading
4. Update `active` flag and `last_chemical_reading_date` in database

### Site Cleanup
- **Unused Site Removal**: Delete sites with no monitoring data in any table
- **Data Validation**: Ensure all sites in database have associated monitoring records

---

## Data Processing After Sites

### Chemical Data Processing
Once sites are established, chemical data is processed with the following approach:

**Multiple Events per Site-Date**: The system allows multiple chemical collection events for the same site and date combination. This preserves all original scientific data including any replicate samples.

**Data Preservation**: All chemical measurements are retained in their original form. If replicate samples exist for the same site and date, they remain as separate collection events in the database.

**Analysis Flexibility**: Having separate replicate events allows for:
- Statistical analysis of measurement variability
- Quality control assessment of sampling procedures
- Transparency in data collection methods

### Data Quality Considerations
- **Validation**: All chemical data is validated against existing sites before insertion
- **Site Matching**: 95% site match rate required before data loading proceeds
- **Error Handling**: Failed site matches are logged and reported for manual review

---

## Usage Examples

### Complete Pipeline Execution
```bash
# Phase 1: Consolidate sites from raw CSVs
python data_processing/consolidate_sites.py

# Phase 2: Merge coordinate duplicates  
python data_processing/merge_sites.py  # Analysis mode
# Review results, then execute:
# merge_duplicate_sites()  # In Python shell

# Phase 3: Load sites and classify
python data_processing/site_processing.py

# Data Loading: Load monitoring data
python database/reset_database.py
```

### Individual Phase Usage
```python
# Phase 1: Just consolidation
from data_processing.consolidate_sites import main
success = main()

# Phase 2: Analysis only
from data_processing.merge_sites import analyze_coordinate_duplicates
analysis = analyze_coordinate_duplicates()

# Phase 3: Database operations
from data_processing.site_processing import process_site_data, classify_active_sites
process_site_data()
classify_active_sites()
```

---

## Decision Logic Summary

### Site Name Standardization
- **Problem**: Inconsistent whitespace and spacing in site names across files
- **Solution**: Strip whitespace and normalize multiple spaces to single spaces
- **Impact**: Enables accurate site matching across different data sources

### Metadata Priority System
- **Problem**: Same sites appear in multiple files with different metadata quality
- **Solution**: Establish clear priority order with conflict detection
- **Impact**: Best available metadata is preserved while flagging inconsistencies

### Coordinate-Based Duplicate Detection
- **Problem**: Same physical locations have different site names
- **Solution**: 3-decimal place coordinate rounding with priority-based selection
- **Impact**: Eliminates duplicate monitoring locations while preserving all data

### Active Site Classification
- **Problem**: Need to distinguish currently monitored sites from historical sites
- **Solution**: 1-year recency cutoff based on chemical monitoring data
- **Impact**: Dashboard can focus on active monitoring while preserving historical context

### Sites First Implementation
- **Problem**: Foreign key constraint violations during data loading
- **Solution**: Ensure all sites exist before loading any monitoring data
- **Impact**: Reliable, error-free data loading with comprehensive validation

---

## Impact on Data Quality

### Before Pipeline:
- Inconsistent site names across data sources
- Duplicate sites for same physical locations
- Mixed metadata quality and completeness
- No distinction between active and historic sites
- Foreign key constraint errors during data loading

### After Pipeline:
- Standardized site names with clean database
- Single site record per physical location
- Best available metadata with source tracking
- Clear active/historic classification
- Complete monitoring data preservation
- Reliable, error-free data loading process

### Key Benefits:
1. **Data Integrity**: All monitoring data is preserved during consolidation
2. **Consistency**: Standardized site names enable accurate cross-referencing
3. **Efficiency**: Eliminates duplicate data entry and confusion
4. **Transparency**: Source tracking shows where each piece of metadata originated
5. **Usability**: Active site classification improves dashboard performance
6. **Reliability**: Sites First approach prevents database constraint violations
7. **Scalability**: Pipeline can handle new data sources and large datasets

### Quality Assurance:
- Conflict detection ensures metadata inconsistencies are flagged for review
- Analysis modes allow preview before making irreversible changes
- Comprehensive logging tracks all decisions and changes made
- Data transfer validation ensures no monitoring records are lost
- Site validation requires 95% match rate before data loading proceeds

---

## Output Files

### Phase 1 Outputs:
- `data/interim/cleaned_*.csv` - Cleaned versions of all raw CSV files
- `data/processed/master_sites.csv` - Consolidated master sites list
- `data/interim/site_conflicts_for_review.csv` - Metadata conflicts requiring manual review

### Phase 2 Outputs:
- Updated database with merged duplicate sites
- Preserved monitoring data under consolidated site records

### Phase 3 Outputs:
- Fully populated sites table in database
- Active/historic classification for all sites
- Clean database ready for monitoring data loading

### Final Database State:
- Complete site registry with consolidated metadata
- All monitoring data loaded and validated
- Active/historic site classification
- Ready for dashboard and analysis use 