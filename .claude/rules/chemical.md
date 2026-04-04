---
description: Chemical data processing — three pathways, BDL handling, duplicate logic
globs:
  - data_processing/chemical_processing.py
  - data_processing/updated_chemical_processing.py
  - data_processing/arcgis_sync.py
  - data_processing/chemical_utils.py
  - cloud_functions/**/chemical_processor.py
  - tests/**/test_chemical*
  - tests/**/test_arcgis*
  - tests/**/test_updated_chemical*
---

# Chemical Data Processing

## Three Pathways (all active, all share `chemical_utils.py`)

1. **`chemical_processing.py`** — Original CSV format, single value per parameter. Uses `cleaned_chemical_data.csv`.
2. **`updated_chemical_processing.py`** — Newer CSV format with Low/Mid/High ranges and selection logic (e.g., greater of two readings, pH furthest from 7.0). Uses `cleaned_updated_chemical_data.csv`. **NOT a newer version of #1** — different data format from a different collection period.
3. **`arcgis_sync.py`** — Real-time FeatureServer sync. Translates field names to match pathway #2's schema, processes through the same pipeline. Uses ArcGIS `objectid` as `sample_id` for idempotent insertion.

## BDL (Below Detection Limit) Handling

- Zero values = below detection limit → replaced with parameter-specific thresholds
- NaN values = actual data gaps → preserved as-is
- Thresholds: Nitrate: 0.3, Nitrite: 0.03, Ammonia: 0.03, Phosphorus: 0.005

## Soluble Nitrogen

Calculated, not measured: Nitrate + Nitrite + Ammonia. Computed during chemical processing.

## Duplicate Handling

Chemical preserves ALL records (no dedup). When `sample_id` is present (FeatureServer data), a partial unique index prevents duplicate events for the same sample. See `docs/decisions/CHEMICAL_DUPLICATE_HANDLING.md` for rationale.

## Key Parameters

| Parameter | Code | Unit | ID | Normal | Caution |
|-----------|------|------|----|--------|---------|
| Dissolved Oxygen | do_percent | % | 1 | 80-130 | 50-150 |
| pH | pH | pH units | 2 | 6.5-9.0 | — |
| Soluble Nitrogen | soluble_nitrogen | mg/L | 3 | <0.8 | <1.5 |
| Phosphorus | Phosphorus | mg/L | 4 | <0.05 | <0.1 |
| Chloride | Chloride | mg/L | 5 | <200 | <400 |

Thresholds defined in `db_schema.py` → `CHEMICAL_REFERENCE_VALUES`.
