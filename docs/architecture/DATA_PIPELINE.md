# Data Processing Pipeline

## Pipeline Execution Order

The ETL pipeline must run in this order — each stage depends on the previous:

```
1. consolidate_sites.py    → Clean CSVs, create master_sites.csv
2. site_processing.py      → Load sites into database
3. merge_sites.py          → Deduplicate sites by coordinates
4. chemical_processing.py  → Process original chemical data
5. updated_chemical_processing.py → Process range-based chemical data
6. fish_processing.py      → Process fish IBI scores (uses bt_fieldwork_validator)
7. macro_processing.py     → Process macroinvertebrate assessments
8. habitat_processing.py   → Process habitat assessments
```

The full pipeline is orchestrated by `database/reset_database.py`.

## File Roles

| File | Purpose |
|------|---------|
| `data_loader.py` | CSV loading, site name cleaning, BDL string conversion, fuzzy site matching (85% threshold) |
| `consolidate_sites.py` | Phase 1: clean raw CSVs → interim/. Phase 2: merge all sites with priority-based metadata resolution |
| `site_processing.py` | Insert/update sites in DB, classify active vs historic (active = chemical reading within 1 year of most recent) |
| `merge_sites.py` | Find coordinate duplicates (3 decimal places, ~111m), merge to preferred site, reassign all monitoring data |
| `chemical_processing.py` | Process `cleaned_chemical_data.csv` — standard single-value chemical measurements |
| `updated_chemical_processing.py` | Process `cleaned_updated_chemical_data.csv` — newer multi-range format (Low/Mid/High) |
| `chemical_utils.py` | Shared chemical constants, validation, BDL conversion, status determination, DB insertion |
| `fish_processing.py` | Fish IBI scores with date correction via `bt_fieldwork_validator` |
| `bt_fieldwork_validator.py` | Validates fish dates against Blue Thumb field work records, detects replicates vs duplicates |
| `macro_processing.py` | Macroinvertebrate metrics grouped by (site, sample_id, habitat, season) |
| `habitat_processing.py` | Habitat assessments with same-date duplicate averaging |
| `biological_utils.py` | Shared utilities for fish and macro: event insertion, sentinel value removal (-999, -99) |
| `data_queries.py` | All database retrieval functions used by the dashboard (pivoted data, status columns, date ranges) |
| `prepare_chatbot_data.py` | Extracts markdown/text content for Vertex AI chatbot knowledge base |

## Two Chemical Pipelines

There are two separate chemical data formats from different collection periods:

- **`chemical_processing.py`** — Original format. Single value per parameter. Uses `cleaned_chemical_data.csv`.
- **`updated_chemical_processing.py`** — Newer format. Parameters measured across Low/Mid/High ranges with a selection column. Uses `cleaned_updated_chemical_data.csv`. Applies range selection logic (e.g., pick greater of two readings, pH furthest from neutral 7.0).

Both pipelines share `chemical_utils.py` for validation, BDL handling, and database insertion.

## Shared Conventions

### Column Name Normalization
Applied at load time across all data types:
- All column names → lowercase
- Spaces and hyphens → underscores
- Special characters removed

### BDL (Below Detection Limit) Handling
- Zero values = below detection limit → replaced with parameter-specific thresholds
- NaN values = actual data gaps → preserved as-is for visualization
- BDL replacement values: Nitrate: 0.3, Nitrite: 0.03, Ammonia: 0.03, Phosphorus: 0.005

### Site Name Matching
- Strip leading/trailing whitespace, collapse multiple spaces
- Exact match required for DB lookups
- Fuzzy matching fallback at 85% similarity (data_loader) or 90% (bt_fieldwork_validator)

### Sentinel Value Removal
- Values of -999 and -99 are treated as missing/placeholder data and removed (biological data)

### Duplicate/Replicate Handling
Each data type handles duplicates differently:
- **Chemical**: All records preserved (no dedup) — see `docs/decisions/CHEMICAL_DUPLICATE_HANDLING.md`
- **Fish**: BT field work records distinguish true replicates from data entry errors — see `docs/decisions/FISH_DATA_VALIDATION.md`
- **Habitat**: Same-date duplicates averaged, grade recalculated — see `docs/decisions/HABITAT_DUPLICATE_HANDLING.md`
- **Macro**: Unique by (site, sample_id, habitat)

## Data Directory Structure

```
data/
├── raw/           → Original source CSV files (never modified)
├── interim/       → Cleaned CSVs (output of consolidate_sites phase 1)
└── processed/     → Database-ready outputs and exports
    └── chatbot_data/  → Sanitized text files for AI knowledge base
```

## Key Chemical Parameters

| Parameter | Code | Unit | ID | Status Thresholds |
|-----------|------|------|----|-------------------|
| Dissolved Oxygen | do_percent | % | 1 | Normal: 80-130, Caution: 50-150 |
| pH | pH | pH units | 2 | Normal: 6.5-9.0 |
| Soluble Nitrogen | soluble_nitrogen | mg/L | 3 | Normal: <0.8, Caution: <1.5 |
| Phosphorus | Phosphorus | mg/L | 4 | Normal: <0.05, Caution: <0.1 |
| Chloride | Chloride | mg/L | 5 | Normal: <200, Caution: <400 |

Soluble Nitrogen = Nitrate + Nitrite + Ammonia (calculated field).

## Domain Glossary

| Term | Meaning |
|------|---------|
| **IBI** | Index of Biotic Integrity — overall fish community health score |
| **BDL** | Below Detection Limit — measurement too low for instrument to detect |
| **EPT** | Ephemeroptera, Plecoptera, Trichoptera — pollution-sensitive insect orders |
| **HBI** | Hilsenhoff Biotic Index — pollution tolerance scoring for macroinvertebrates |
| **do_percent** | Dissolved oxygen as percent saturation |
| **Integrity class** | Fish health rating: Excellent/Good/Fair/Poor/Very Poor |
| **Biological condition** | Macro health: Non-impaired/Slightly/Moderately/Severely Impaired |
| **Habitat grade** | Physical stream condition: A/B/C/D/F (90/80/70/60/<60) |
| **Replicate** | Legitimate multiple samples at same site in same year (different dates) |
| **Duplicate** | Data entry error — multiple records for same site on same date |
| **BT field work** | Blue Thumb authoritative field records used for fish date validation |
