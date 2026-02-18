# Fish Data Validation and Date Correction

## Overview
The fish data validation system provides comprehensive date correction and duplicate handling for fish collection data. It uses Blue Thumb field work records as the authoritative source for collection dates when available, and falls back to database YEAR fields as truth when no BT match exists. The system distinguishes between true replicate samples and duplicate data entries using **date-based detection** from BT field work documentation.

## Two-Tier Validation System

### 1. Comprehensive Date Correction (Primary)
**Purpose**: Ensure all fish records have consistent collection_date and year fields
**Scope**: ALL fish records in the database
**Approach**: BT data as authoritative source, with YEAR field fallback

### 2. Date-Based Replicate Processing (Secondary)  
**Purpose**: Handle multiple samples per site+year combination
**Scope**: Only records with multiple samples for same site+year
**Approach**: **Date-based detection** - multiple BT dates = replicates, single BT date = duplicates

---

## Comprehensive Date Correction Process

### The Date Consistency Problem

**Challenge**: Many fish records have mismatched collection_date and year fields
- Collection date shows one year (e.g., 2016-07-28) 
- Year field shows different year (e.g., 2019)
- Causes visualization inconsistencies between line graphs and tables

**Impact**: 
- Line graphs use collection_date → show wrong temporal positioning
- Tables use year field → show different year groupings
- Result: Same data appears different in different visualizations

### Solution Strategy

**Two-tier correction approach:**

1. **Primary**: Use BT field work data as authoritative source when available
2. **Fallback**: Use YEAR field as truth when no BT match exists

### Data Source
- **File**: `BT_fish_collection_dates.csv`
- **Coverage**: 239 unique BT sites vs 169 fish database sites
- **Match Rate**: ~75% direct matches between fish sites and BT sites
- **Authority**: BT data represents actual field work collection dates

### Correction Process

#### Step 1: Identify Mismatches
```python
# Find records where year field doesn't match collection_date year
fish_df['year_from_date'] = fish_df['collection_date'].dt.year
mismatched_mask = fish_df['year'] != fish_df['year_from_date']
```

#### Step 2: Apply BT Corrections (When Available)
For each mismatched record:
1. **Find BT Site Match**: Use exact matching (75% success rate)
2. **Search BT Data**: Look for collections matching either the database year or date year
3. **Apply BT Date**: Use BT collection date as authoritative source
4. **Update Both Fields**: Set both collection_date and year to match BT data

```python
# BT correction logic
bt_matches = bt_df[
    (bt_df['Site_Clean'] == bt_site_match) & 
    (bt_df['Year'].isin([db_year, date_year]))
]

if not bt_matches.empty:
    bt_date = bt_matches.iloc[0]['Date_Clean']
    fish_corrected.at[idx, 'collection_date'] = bt_date
    fish_corrected.at[idx, 'year'] = bt_date.year
    correction_source = "BT_truth"
```

#### Step 3: Apply YEAR Field Fallback
For records without BT matches:
1. **Trust YEAR Field**: Assume database year field is correct
2. **Preserve Month/Day**: Keep original month and day from collection_date
3. **Update Collection Date**: Replace only the year component

```python
# Year field fallback logic
corrected_date = original_date.replace(year=db_year)
fish_corrected.at[idx, 'collection_date'] = corrected_date
correction_source = "year_field_truth"
```

### Correction Results

**Typical Processing Summary:**
```
Found 44 records with year/date mismatches
Date correction complete: 44 corrections applied
  - BT truth corrections: 23 (52.3%)
  - Year field corrections: 21 (47.7%)
All date mismatches successfully resolved
```

**Benefits:**
- **100% Resolution**: All date mismatches corrected
- **Authoritative Source**: BT data used when available (52% of cases)
- **Practical Fallback**: YEAR field trusted when no BT data (48% of cases)
- **Perfect Consistency**: Visualizations now show identical temporal data

---

## Site Matching Logic

### Matching Strategy
**Two-tier approach for finding BT site matches:**

1. **Exact Match**: Direct site name match after cleaning
2. **Fuzzy Match**: Similarity-based matching (90% threshold) for name variations

```python
def find_bt_site_match(db_site_name, bt_sites, threshold=0.9):
    # Try exact match first
    if db_site_name in bt_sites:
        return db_site_name
    
    # Fall back to fuzzy matching for variants
    best_score = difflib.SequenceMatcher(None, db_site_name.lower(), bt_site.lower()).ratio()
    if best_score > threshold:
        return best_match
```

### Coverage Analysis
- **Fish Sites**: 169 unique sites in database
- **BT Sites**: 239 unique sites in field work records  
- **Direct Matches**: 127 sites (75% coverage)
- **No Match**: 42 fish sites (25%) - use YEAR field fallback

### Common Mismatch Patterns
- Minor punctuation differences: "Hwy 51" vs "Hwy. 51"
- Road designation variations: "N 4370 Rd" vs "N 4370 Rd."
- Abbreviation differences: "Creek" vs "Cr."
- Additional descriptors: "Site A" vs "Site A (upstream)"

---

## Date-Based Replicate/Duplicate Processing

### The Replicate vs Duplicate Problem

**Challenge**: Multiple samples for same site+year may represent:
- **True Replicates**: Separate collection events on different dates
- **Data Duplicates**: Multiple entries of same collection event

**Solution**: Use **date-based detection** from BT field work records (replaces legacy REP label system)

### Date-Based Replicate Detection

**New Logic** (replaces REP label detection):
1. **Check BT Data**: Look for multiple collection dates for same site+year
2. **Multiple Dates Found**: Treat as replicates - assign actual BT dates chronologically
3. **Single/No Dates Found**: Treat as duplicates - average the samples

### Replicate Detection Process

#### Step 1: Site Matching
1. **Find BT Match**: Use same fuzzy matching as date correction
2. **Year Buffer Search**: Check target year ±1 for collection dates
3. **Count Dates**: Determine if multiple dates exist

```python
def detect_replicates_by_dates(bt_df, site_name, year):
    # Find BT site match
    bt_site_match = find_bt_site_match(site_name, bt_sites)
    
    # Look for multiple dates in target year ±1 buffer
    for check_year in [year, year-1, year+1]:
        potential_dates = bt_df[
            (bt_df['Site_Clean'] == bt_site_match) & 
            (bt_df['Year'] == check_year)
        ]
        
        if len(potential_dates) >= 2:
            # Multiple dates found = replicates
            return potential_dates.sort_values('Date_Clean')
    
    return None  # No multiple dates = duplicates
```

#### Step 2: Processing Logic

**For Multiple BT Dates (Replicates):**
1. **Sort BT Dates**: Order dates chronologically 
2. **Sort Fish Samples**: Order database samples by sample_id
3. **Assign Dates**: First sample gets earliest BT date, second gets later BT date
4. **Update Records**: Set collection_date and year to match BT dates

**For Single/No BT Dates (Duplicates):**
1. **Average Metrics**: Calculate mean `comparison_to_reference` 
2. **Nullify Scores**: Individual 1,3,5 scale scores not meaningful when averaged
3. **Remove Originals**: Delete original duplicate records
4. **Insert Average**: Add single averaged record

### Processing Examples

#### Example 1: Spring Creek I-35 (2006) - True Replicates
```
BT Data Found:
  2006-06-06 (Fish)
  2006-08-28 (Fish)

Processing Result:
  Sample 35151 → 2006-06-06 (original)
  Sample 36254 → 2006-08-28 (replicate)

Status: REPLICATES - Multiple BT dates found
```

#### Example 2: Coal Creek Hwy 11 (2012) - Duplicates  
```
BT Data Found:
  2012-06-11 (Fish)
  2012-06-14 (Fish)
  
Processing Result:
  Multiple samples averaged into single record
  All individual scores nullified, only comparison_to_reference averaged

Status: DUPLICATES - Multiple dates but no distinct collection events
```

---

## Complete Processing Workflow

### Processing Order
1. **Load Data**: Fish data and BT field work records
2. **Comprehensive Date Correction**: Fix ALL date mismatches using BT+YEAR approach
3. **Date-Based Replicate Processing**: Handle multiple samples per site+year using date detection
4. **Data Validation**: Standard biological data validation
5. **Database Insertion**: Store corrected and processed data

### Workflow Implementation
```python
def process_fish_csv_data(site_name=None):
    # Load and map fish data
    fish_df = load_csv_data('fish', parse_dates=['Date'])
    fish_df = clean_and_map_columns(fish_df)
    
    # STEP 1: Comprehensive date correction
    bt_df = load_bt_field_work_dates()
    fish_df = correct_collection_dates(fish_df, bt_df)
    
    # STEP 2: Date-based replicate/duplicate processing
    fish_df = categorize_and_process_duplicates(fish_df, bt_df)
    
    # STEP 3: Standard validation
    fish_df = remove_invalid_biological_values(fish_df)
    fish_df = convert_columns_to_numeric(fish_df)
    fish_df = validate_ibi_scores(fish_df)
    
    return fish_df
```

### Processing Statistics Example
```
=== Comprehensive Date Correction ===
Found 44 records with year/date mismatches
Date correction complete: 44 corrections applied
  - BT truth corrections: 23 (52.3%)
  - Year field corrections: 21 (47.7%)
All date mismatches successfully resolved

=== Date-Based Replicate/Duplicate Processing ===
Fish duplicate processing (date-based): 11 replicate groups, 13 groups averaged, 22 date assignments

=== Final Results ===
Original records: 381
Final records: 342 (39 fewer due to duplicate averaging)
Remaining date mismatches: 0
```

---

## Usage and Integration

### Basic Usage
```python
from data_processing.bt_fieldwork_validator import correct_collection_dates, categorize_and_process_duplicates

# Comprehensive date correction
corrected_df = correct_collection_dates(fish_df)

# Date-based replicate/duplicate processing  
processed_df = categorize_and_process_duplicates(corrected_df, bt_df)
```

### Quality Assurance Features

#### Correction Tracking
```python
correction_log.append({
    'site_name': site_name,
    'sample_id': sample_id,
    'original_date': original_date.strftime('%Y-%m-%d'),
    'corrected_date': new_date.strftime('%Y-%m-%d'),
    'correction_source': 'BT_truth' or 'year_field_truth',
    'bt_site_match': bt_site_match or 'no_match'
})
```

#### Validation Checks
- **Pre-correction**: Count initial mismatches
- **Post-correction**: Verify zero remaining mismatches
- **Coverage Analysis**: Track BT vs YEAR field correction ratios
- **Audit Trail**: Log all correction decisions and replicate classifications

---

## Impact on Visualization System

### Before Comprehensive Correction:
- **Line Graphs**: Used collection_date (inconsistent years)
- **Tables**: Used year field (different temporal grouping) 
- **Result**: Same data appeared different across visualizations
- **Example**: Wolf Creek showed 3 samples in graph, 2 in 2016 + 1 in 2019 in table

### After Comprehensive Correction:
- **Perfect Consistency**: Line graphs and tables show identical data
- **Temporal Accuracy**: All dates reflect authoritative sources
- **Data Integrity**: Zero remaining year/date mismatches
- **Proper Replicates**: True replicates shown with REP notation, duplicates properly averaged

### Visualization Results
```python
# Spring Creek: I-35 - Now shows proper replicates
Table Display:
  2006: IBI=13, Poor (Sample 35151, 2006-06-06)
  2006 (REP): IBI=13, Poor (Sample 36254, 2006-08-28)

Line Graph:
  2006-06-06: IBI=13, Poor
  2006-08-28: IBI=13, Poor

Consistency: ✅ PERFECT MATCH - Shows 2 replicates in 2006
```

---

## Decision Logic Summary

### Comprehensive Date Correction Decision Tree

```
Fish record loaded?
├─ collection_date year ≠ year field?
│   ├─ YES: Find BT site match
│   │   ├─ BT match found?
│   │   │   ├─ YES: BT data for either year available?
│   │   │   │   ├─ YES: → Use BT date (authoritative)
│   │   │   │   └─ NO: → Use YEAR field (keep month/day)
│   │   │   └─ NO: → Use YEAR field (keep month/day)
│   │   └─ Continue to replicate processing...
│   └─ NO: → Date consistent, continue processing
└─ NO: → Error handling
```

### Date-Based Replicate Processing Decision Tree

```
Multiple samples for same site+year?
├─ YES: Check BT field work records  
│   ├─ BT site match found?
│   │   ├─ YES: Multiple dates in BT data (±1 year)?
│   │   │   ├─ YES: → REPLICATES (assign BT dates chronologically)
│   │   │   └─ NO: → DUPLICATES (average samples)
│   │   └─ NO: → DUPLICATES (average samples)
│   └─ BT data unavailable: → DUPLICATES (average samples)
└─ NO: → Single sample (no processing needed)
```

---

## Key Improvements in New System

### Enhanced Replicate Detection
1. **Date-Based Logic**: Uses actual collection dates instead of REP labels
2. **Comprehensive Coverage**: Processes ALL sites regardless of labeling
3. **Accurate Classification**: Spring Creek I-35 now properly identified as replicates
4. **Chronological Assignment**: Earlier sample = original, later sample = REP

### Improved Data Integrity
1. **Universal Coverage**: ALL fish records checked for date consistency
2. **Authoritative Sources**: BT data prioritized when available
3. **Intelligent Fallbacks**: YEAR field trusted when no BT data
4. **Perfect Resolution**: 100% of date mismatches corrected

### Better Visualization Consistency  
1. **Unified Data**: Line graphs and tables use identical temporal data
2. **Accurate Positioning**: Collection dates reflect true field work
3. **Proper REP Notation**: True replicates displayed with (REP) suffix
4. **Quality Assurance**: Zero tolerance for date mismatches

### Comprehensive Processing Pipeline
1. **Date Correction First**: Fix fundamental data integrity issues
2. **Date-Based Replicate Processing**: Handle special cases using actual collection dates
3. **Standard Validation**: Apply biological data validation rules
4. **Full Audit Trail**: Track all processing decisions and corrections

This comprehensive system ensures that fish community data has both temporal accuracy and internal consistency, with proper identification of replicate vs duplicate samples based on actual field work documentation rather than potentially inconsistent labeling.