# API-First Chemical Data Pipeline

## Overview

Replace the CSV-based `updated_chemical_processing.py` pipeline with an API-first approach using the ArcGIS Feature Server. This eliminates the stale CSV dependency for current-period chemical data (post-2020, ~3,271 records) while keeping the legacy CSV pipeline (`chemical_processing.py`) for pre-2020 data.

**Motivation:** The `updated_chemical_data.csv` is a manual export that's already 532 records and 11 sites behind the live Feature Server. The current `arcgis_sync.py` translates API field names to CSV column names just so the CSV-era processing functions can handle them — a double translation (API → CSV → DB) that exists only because the pipeline was built for CSV first. Going API-first collapses this to a single hop (API → DB) and makes the Feature Server the sole source of truth for current-period data.

**Relationship to PR #15:** This complements the unknown-sites scaffolding in PR #15. By pulling sites from the Feature Server during `reset_database`, new monitoring locations (like the 4 unknown sites identified in PR #15) get registered automatically on every reset. PR #15 still handles the edge case of sites appearing between resets. Either PR can merge first — no conflicts.

## Scope

### What changes

- **`arcgis_sync.py`** — becomes the single module for current-period chemical data. Processing logic (nutrient selection, pH worst-case, date parsing) is rewritten to use API field names directly. `ARCGIS_FIELD_MAP` is replaced with a minimal direct-to-DB mapping.
- **`consolidate_sites.py`** — priority 4 slot fetches distinct sites from the Feature Server (site name, lat/lon from geometry, county) instead of reading `cleaned_updated_chemical_data.csv`. Consolidation priority order and merge logic unchanged.
- **`reset_database.py`** — Phase 2 calls a new `sync_all_chemical_data()` function in `arcgis_sync.py` instead of `load_updated_chemical_data_to_db()`.
- **`clean_all_csvs()` / `verify_cleaned_csvs()`** — drop `updated_chemical_data.csv` from their file lists.

### What gets retired

- **`updated_chemical_processing.py`** — deleted entirely
- **`data/raw/updated_chemical_data.csv`** — no longer needed
- All references to `cleaned_updated_chemical_data.csv`

### What stays unchanged

- `chemical_processing.py` — legacy pre-2020 CSV pipeline
- `chemical_utils.py` — shared validation, BDL conversion, insertion utilities
- Cloud Function daily sync (`sync_new_chemical_data`)
- All other CSVs (site_data, chemical_data, fish, macro, habitat)

## Data Flow

### Current (3 hops)

```
Feature Server → ARCGIS_FIELD_MAP → CSV column names → format_to_database_schema → DB columns
```

### New (1 hop)

```
Feature Server → processing functions (API field names) → DB columns
```

### Field mapping

| API Field | Processing Step | DB Column |
|---|---|---|
| `SiteName` | rename | `Site_Name` |
| `day` (epoch ms) | convert to date | `Date`, `Year`, `Month` |
| `oxygen_sat` | rename | `do_percent` |
| `pH1`, `pH2` | worst-case (furthest from 7) | `pH` |
| `nitratetest1`, `nitratetest2` | greater value | `Nitrate` |
| `nitritetest1`, `nitritetest2` | greater value | `Nitrite` |
| `Ammonia_Range` + `ammonia_Nitrogen2`, `ammonia_Nitrogen3`, `Ammonia_nitrogen_midrange1_Final`, `Ammonia_nitrogen_midrange2_Final` | conditional by range | `Ammonia` |
| `Ortho_Range` + `Orthophosphate_Low1_Final` through `Orthophosphate_High2_Final` | conditional by range, rename | `Phosphorus` |
| `Chloride_Range` + `Chloride_Low1_Final` through `Chloride_High2_Final` | conditional by range | `Chloride` |
| (derived) | Nitrate + Nitrite + Ammonia | `soluble_nitrogen` |

The final rename mapping is minimal:

```python
COLUMN_TO_DB = {
    'SiteName': 'Site_Name',
    'oxygen_sat': 'do_percent',
    'Orthophosphate': 'Phosphorus',
}
```

### Date parsing simplification

Current `arcgis_sync.py` converts epoch ms → formatted string (`'%m/%d/%Y, %I:%M %p'`) so `parse_sampling_dates()` can parse it back. The new version skips the string round-trip and converts epoch ms directly to a date with Central timezone handling.

## Site Consolidation

### Priority order (unchanged logic)

```
Priority 1: site_data.csv          → extract_sites_from_csv()
Priority 2: chemical_data.csv      → extract_sites_from_csv()
Priority 3: fish_data.csv          → extract_sites_from_csv()
Priority 4: Feature Server API     → extract_sites_from_feature_server()  ← new
Priority 5: macro_data.csv         → extract_sites_from_csv()
Priority 6: habitat_data.csv       → extract_sites_from_csv()
```

### Feature Server site extraction

`extract_sites_from_feature_server()` queries the Feature Server for all features with `returnGeometry=true`, extracts:
- `SiteName` → `site_name`
- Geometry `y` → `latitude`
- Geometry `x` → `longitude`
- `CountyName` → `county`
- `river_basin` → None (not available)
- `ecoregion` → None (not available)

Deduplicates by site name (takes first coordinate pair per site, logs warning if coordinates differ for same site name). Returns a DataFrame in the same format as `extract_sites_from_csv()` so the merge logic is untouched.

`source_file` is set to `'arcgis_feature_server'` for provenance tracking.

### Feature Server metadata coverage

The Feature Server provides the same metadata as `updated_chemical_data.csv` did at priority 4: site name, lat, lon, and county. No basin or ecoregion — same as before. Lower-priority sources (macro, habitat) continue to fill in those fields.

## Reset Database Changes

### New entry point: `sync_all_chemical_data()`

A new function in `arcgis_sync.py` for the full-fetch reset case. This is distinct from `sync_new_chemical_data()` (incremental, date-filtered, used by Cloud Function). The difference is only the `where` clause — `sync_all_chemical_data` fetches all QAQC-complete records with no date filter.

Both share the same processing pipeline and insertion logic.

### Error handling

If the Feature Server is unreachable during reset, the function raises with a clear error message. No silent fallback. Network access is guaranteed on Cloud Run; for local development, internet is a reasonable requirement for a database reset.

## Verification

### Feature Server stats (verified 2026-04-01)

| Metric | Value |
|---|---|
| Total records | 3,271 |
| QAQC complete | 3,271 (100%) |
| Distinct sites | 163 |
| Date range | Mar 2006 – Apr 2026 |
| Pre-2020 records | 15 (0.5%) |
| Post-2020 records | ~3,256 (99.5%) |
