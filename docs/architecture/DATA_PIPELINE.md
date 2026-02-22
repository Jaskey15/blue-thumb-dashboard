# Data Processing Pipeline

## Pipeline Execution Order

The ETL pipeline must run in this order — each stage depends on the previous:

```
1. consolidate_sites.py    → Clean CSVs, create master_sites.csv
2. site_processing.py      → Load sites into database
3. merge_sites.py          → Deduplicate sites by coordinate proximity (Haversine clustering)
4. chemical_processing.py  → Process original chemical data
5. updated_chemical_processing.py → Process range-based chemical data
6. fish_processing.py      → Process fish IBI scores (uses bt_fieldwork_validator)
7. macro_processing.py     → Process macroinvertebrate assessments
8. habitat_processing.py   → Process habitat assessments
```

The full pipeline is orchestrated by `database/reset_database.py`.

## Real-Time Data Ingestion

In addition to the batch CSV pipeline above, chemical data is also ingested in real-time from the ArcGIS FeatureServer via `data_processing/arcgis_sync.py`. This module:

1. Fetches QAQC-verified records from the public FeatureServer REST API (no authentication required)
2. Translates FeatureServer field names to the CSV column names the existing pipeline expects
3. Runs translated data through the same chemical processing pipeline as CSV data
4. Inserts into the database using `sample_id`-based idempotent insertion (prevents duplicates on re-sync)

Two fetch strategies are supported:
- **Date-based** (`fetch_features_since`) — fetches by sampling date (`day` field). Used for initial sync.
- **EditDate-based** (`fetch_features_edited_since`) — fetches by last-edited timestamp. Used for incremental syncs after the first run.

The Cloud Function orchestrates this via `mode=feature_server` (see Deployment docs).

## File Roles

| File | Purpose |
|------|---------|
| `data_loader.py` | CSV loading, site name cleaning, BDL string conversion, fuzzy site matching (85% threshold) |
| `consolidate_sites.py` | Phase 1: clean raw CSVs → interim/. Phase 2: merge all sites with priority-based metadata resolution |
| `site_processing.py` | Insert/update sites in DB, classify active vs historic (active = chemical reading within 1 year of most recent) |
| `merge_sites.py` | Find coordinate duplicates via boundary-safe Haversine clustering (50m default threshold, floor-bin + neighbor-bin expansion, union-find transitive grouping). Merge to preferred site, reassign all monitoring data. Legacy rounding mode available via `boundary_safe=False` |
| `chemical_processing.py` | Process `cleaned_chemical_data.csv` — standard single-value chemical measurements |
| `updated_chemical_processing.py` | Process `cleaned_updated_chemical_data.csv` — newer multi-range format (Low/Mid/High) |
| `chemical_utils.py` | Shared chemical constants, validation, BDL conversion, status determination, DB insertion. Supports `sample_id`-based idempotent event insertion |
| `arcgis_sync.py` | Real-time FeatureServer sync: fetch, translate field names, normalize sites, process, and insert chemical data from the public ArcGIS endpoint |
| `fish_processing.py` | Fish IBI scores with date correction via `bt_fieldwork_validator` |
| `bt_fieldwork_validator.py` | Validates fish dates against Blue Thumb field work records, detects replicates vs duplicates |
| `macro_processing.py` | Macroinvertebrate metrics grouped by (site, sample_id, habitat, season) |
| `habitat_processing.py` | Habitat assessments with same-date duplicate averaging |
| `biological_utils.py` | Shared utilities for fish and macro: event insertion, sentinel value removal (-999, -99) |
| `data_queries.py` | All database retrieval functions used by the dashboard (pivoted data, status columns, date ranges) |
| `prepare_chatbot_data.py` | Extracts markdown/text content for Vertex AI chatbot knowledge base |

## Three Chemical Data Pathways

There are three chemical data ingestion pathways:

- **`chemical_processing.py`** — Original CSV format. Single value per parameter. Uses `cleaned_chemical_data.csv`.
- **`updated_chemical_processing.py`** — Newer CSV format. Parameters measured across Low/Mid/High ranges with a selection column. Uses `cleaned_updated_chemical_data.csv`. Applies range selection logic (e.g., pick greater of two readings, pH furthest from neutral 7.0).
- **`arcgis_sync.py`** — Real-time FeatureServer sync. Fetches records from the public ArcGIS FeatureServer, translates field names to match the `updated_chemical_processing` pipeline schema, and processes through the same pipeline. Uses `objectid` as `sample_id` for idempotent insertion.

All three pathways share `chemical_utils.py` for validation, BDL handling, and database insertion.

## Site Deduplication

Sites with nearly identical coordinates are merged in step 3 of the pipeline (`merge_sites.py`). The default algorithm uses **boundary-safe Haversine clustering**:

1. **Candidate generation**: Coordinates are binned using `floor(lat * 1000)` / `floor(lon * 1000)` (~0.001° bins). Each site is compared against sites in the same bin and the 8 neighboring bins (±1 in lat/lon).
2. **Distance filtering**: Candidate pairs are filtered by Haversine distance (default threshold: 50m).
3. **Transitive clustering**: Union-find groups connected pairs transitively — if A is near B and B is near C, all three form one cluster.
4. **Preferred site selection**: Within each cluster, the preferred site is chosen by priority:
   - Sites present in `updated_chemical_data` source file (highest priority)
   - Sites present in `chemical_data` source file
   - Longest site name (fallback)
5. **Merge**: All monitoring data (chemical, fish, macro, habitat) is reassigned from duplicate sites to the preferred site, then duplicates are deleted. Cleaned interim CSVs are updated with the new site name mappings.

A legacy rounding mode (`boundary_safe=False`) groups by identical `ROUND(latitude, 3)` / `ROUND(longitude, 3)` bins but can miss near-duplicates on rounding boundaries. See `docs/RFC_PIPELINE_HARDENING_VALIDATION.md` for the design rationale.

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
- **Sites**: Coordinate-proximity dedup via boundary-safe Haversine clustering (see Site Deduplication section below)
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
