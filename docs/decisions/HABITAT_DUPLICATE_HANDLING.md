# Habitat Data Duplicate Handling

## Overview
The habitat data processing system handles duplicate assessments by averaging all numeric metrics and scores for samples collected at the same site on the same date. Unlike fish and chemical data processing, habitat data lacks an authoritative external source for date validation, so the system relies on **same-date averaging** as the primary duplicate resolution strategy.

## Key Differences from Other Data Types

### Habitat vs Fish Data Processing
- **Fish Data**: Uses Blue Thumb field work records (`BT_fish_collection_dates.csv`) as authoritative source for collection dates
- **Fish Data**: Distinguishes between true replicates (different dates) and duplicates (same date) using BT data
- **Habitat Data**: **No external date authority** - relies solely on database dates for duplicate detection
- **Habitat Data**: All samples with same site+date are treated as duplicates and averaged

### Habitat vs Chemical Data Processing  
- **Chemical Data**: Allows duplicates during insertion, then consolidates using "worst case" logic
- **Chemical Data**: Separate consolidation step with dry-run capabilities
- **Habitat Data**: **Immediate duplicate resolution** during CSV processing
- **Habitat Data**: **Averaging approach** rather than worst-case selection

## Habitat Duplicate Resolution Process

### The Habitat Duplication Problem

**Challenge**: Multiple habitat assessments may exist for the same site and date
- Same site assessed multiple times on same date
- Multiple assessors providing independent scores
- Data entry errors creating duplicate records

**Impact**: 
- Inflated sample counts in visualizations
- Inconsistent habitat grades for same site+date
- Skewed analysis results due to duplicate weighting

### Solution Strategy

**Same-Date Averaging Approach:**
1. **Group by Site+Date**: Identify all assessments for same site and assessment date
2. **Average Metrics**: Calculate mean values for all 11 habitat parameters (1-20 scale each)
3. **Average Total Score**: Calculate mean total score and round to nearest integer
4. **Recalculate Grade**: Determine habitat grade based on averaged total score
5. **Single Record**: Replace duplicates with single averaged assessment

### Data Processing

#### Step 1: Identify Duplicate Groups
```python
# Group by site and date to find duplicates
grouped = habitat_df.groupby(['site_name', 'assessment_date'])

for (site_name, date_str), group in grouped:
    if len(group) > 1:
        # Multiple assessments found for same site+date
        duplicate_groups.append((site_name, date_str, group))
```

#### Step 2: Calculate Averages
For each duplicate group:
1. **Individual Metrics**: Average all 11 habitat parameters, round to 1 decimal place
2. **Total Score**: Average total scores, round to nearest integer  
3. **Habitat Grade**: Recalculate grade based on averaged total score

```python
# Average individual metrics (round to 1 decimal place)
for col in existing_metric_columns:
    values = group[col].dropna()
    if len(values) > 0:
        avg_value = values.mean()
        averaged_record[col] = round(avg_value, 1)

# Average total score (round to nearest integer)
if 'total_score' in habitat_df.columns:
    total_values = group['total_score'].dropna()
    if len(total_values) > 0:
        avg_total = total_values.mean()
        averaged_record['total_score'] = round(avg_total)

# Calculate new habitat grade based on averaged total score
if pd.notna(averaged_record['total_score']):
    averaged_record['habitat_grade'] = calculate_habitat_grade(averaged_record['total_score'])
```

#### Step 3: Grade Calculation
Habitat grades are calculated on a 1-100 scale:
- **A**: 90-100 (Excellent)
- **B**: 80-89 (Good) 
- **C**: 70-79 (Fair)
- **D**: 60-69 (Poor)
- **F**: 0-59 (Very Poor)

### Habitat Metrics Processed

**11 Habitat Parameters** (each scored 1-20):
1. **Instream Cover**: Available cover for fish
2. **Pool Bottom Substrate**: Quality of pool substrates
3. **Pool Variability**: Diversity of pool characteristics
4. **Canopy Cover**: Riparian vegetation coverage
5. **Rocky Runs/Riffles**: Presence and quality of riffle habitat
6. **Flow**: Stream flow characteristics
7. **Channel Alteration**: Degree of channel modification
8. **Channel Sinuosity**: Natural meandering patterns
9. **Bank Stability**: Condition of stream banks
10. **Bank Vegetation Stability**: Riparian vegetation health
11. **Streamside Cover**: Terrestrial cover near stream

**Total Score**: Sum of all parameters (maximum 220, converted to 1-100 scale)

## Processing Workflow

### Complete Processing Pipeline
```python
def process_habitat_csv_data(site_name=None):
    # Load and clean habitat data
    habitat_df = load_csv_data('habitat')
    habitat_df = clean_column_names(habitat_df)
    habitat_df = habitat_df.rename(columns=valid_mapping)
    
    # RESOLVE DUPLICATES - Core processing step
    habitat_df = resolve_habitat_duplicates(habitat_df)
    
    # Handle date formatting and site filtering
    habitat_df = process_dates_and_filtering(habitat_df, site_name)
    
    return habitat_df
```

### Duplicate Resolution Function
```python
def resolve_habitat_duplicates(habitat_df):
    """
    Resolve habitat duplicate assessments by averaging all numeric metrics and scores.
    
    Returns:
        DataFrame with duplicates resolved through averaging
    """
    # Define metric columns for averaging
    metric_columns = [
        'instream_cover', 'pool_bottom_substrate', 'pool_variability',
        'canopy_cover', 'rocky_runs_riffles', 'flow', 'channel_alteration',
        'channel_sinuosity', 'bank_stability', 'bank_vegetation_stability',
        'streamside_cover'
    ]
    
    # Group by site and date
    grouped = habitat_df.groupby(['site_name', 'assessment_date'])
    
    # Process each group
    for (site_name, date_str), group in grouped:
        if len(group) > 1:
            # Calculate averages for duplicate group
            averaged_record = calculate_averaged_record(group, metric_columns)
            averaged_records.append(averaged_record)
    
    # Combine unique records with averaged records
    return combine_unique_and_averaged_records(unique_records, averaged_records)
```

## Processing Results

### Typical Processing Summary
```
Habitat duplicate resolution: 12 duplicates resolved from 156 records
Final habitat data: 144 records (12 fewer due to duplicate averaging)
  - Individual metrics averaged and rounded to 1 decimal place
  - Total scores averaged and rounded to nearest integer
  - Habitat grades recalculated based on averaged totals
```

### Quality Assurance
- **Pre-processing**: Count initial records and identify duplicate groups
- **Post-processing**: Verify reduction in record count matches duplicates resolved
- **Metric Validation**: Ensure all averaged values remain within valid ranges (1-20 for individuals, 1-100 for totals)
- **Grade Consistency**: Confirm habitat grades align with averaged total scores

## Database Integration

### Storage Strategy
1. **Habitat Assessments**: One record per unique site+date combination (post-averaging)
2. **Individual Metrics**: Store averaged metric values with 1 decimal precision
3. **Summary Scores**: Store averaged total scores as integers with recalculated grades

### Database Tables
```sql
-- Habitat assessments (one per site+date after averaging)
habitat_assessments (assessment_id, site_id, assessment_date, year)

-- Individual metrics (averaged values)
habitat_metrics (assessment_id, metric_name, score)

-- Summary scores (averaged totals with recalculated grades)  
habitat_summary_scores (assessment_id, total_score, habitat_grade)
```

## Decision Logic Summary

### Habitat Duplicate Processing Decision Tree
```
Habitat record loaded?
├─ Multiple assessments for same site+date?
│   ├─ YES: Calculate averages
│   │   ├─ Average individual metrics (round to 1 decimal)
│   │   ├─ Average total score (round to integer)
│   │   ├─ Recalculate habitat grade based on averaged total
│   │   └─ Replace duplicates with single averaged record
│   └─ NO: Keep original record unchanged
└─ Continue to database insertion
```

### Key Decision Rationale
**Why Same-Date Averaging?**
1. **No External Authority**: Unlike fish data, no Blue Thumb field work records exist for habitat assessments
2. **Assessment Consistency**: Multiple assessors on same date should produce similar results
3. **Data Quality**: Averaging reduces impact of outlier assessments or scoring inconsistencies
4. **Simplicity**: Straightforward approach without complex date validation requirements

## Benefits and Limitations

### Benefits
1. **Data Consistency**: Eliminates duplicate assessments for same site+date
2. **Quality Improvement**: Averaging reduces impact of individual assessor bias
3. **Simple Implementation**: No external data dependencies or complex validation rules
4. **Reliable Results**: Consistent habitat grades based on averaged metrics

### Limitations
1. **No Date Validation**: Cannot distinguish between legitimate replicates vs duplicates
2. **Information Loss**: May average out meaningful variation between assessments
3. **Assumption-Based**: Assumes all same-date assessments are duplicates rather than replicates
4. **No Audit Trail**: Original individual assessments are lost in averaging process

## Comparison with Other Data Types

| Aspect | Habitat Data | Fish Data | Chemical Data |
|--------|--------------|-----------|---------------|
| **External Authority** | None | BT field work records | None |
| **Duplicate Detection** | Same site+date | Date-based with BT validation | Same site+date |
| **Resolution Strategy** | Averaging | Replicate vs duplicate logic | Worst-case consolidation |
| **Processing Timing** | During CSV processing | During CSV processing | Post-insertion consolidation |
| **Data Preservation** | Averages replace originals | Replicates preserved, duplicates averaged | Originals deleted, consolidated values stored |
| **Quality Assurance** | Averaging validation | Date correction + replicate detection | Dry-run + consolidation verification |

## Usage Examples

### Basic Duplicate Resolution
```python
from data_processing.habitat_processing import resolve_habitat_duplicates

# Process habitat data with duplicate resolution
habitat_df = load_csv_data('habitat')
resolved_df = resolve_habitat_duplicates(habitat_df)

print(f"Original records: {len(habitat_df)}")
print(f"After duplicate resolution: {len(resolved_df)}")
```

### Complete Processing Pipeline
```python
from data_processing.habitat_processing import load_habitat_data

# Load and process all habitat data
habitat_df = load_habitat_data()

# Or process specific site
site_habitat_df = load_habitat_data(site_name="Spring Creek: I-35")
```

## Impact on Visualization System

### Before Duplicate Resolution:
- **Inflated Counts**: Multiple assessments shown for same site+date
- **Inconsistent Grades**: Different habitat grades for same location+date
- **Analysis Bias**: Duplicate records weighted multiple times in calculations

### After Duplicate Resolution:
- **Accurate Representation**: Single assessment per site+date combination
- **Consistent Grading**: Unified habitat grade based on averaged metrics
- **Proper Analysis**: Each site+date weighted once in statistical calculations
- **Clean Visualizations**: No duplicate entries cluttering charts and tables

This habitat duplicate handling system ensures data quality and consistency while acknowledging the limitations of having no external authoritative source for date validation, making it well-suited for the Blue Thumb habitat assessment workflow. 