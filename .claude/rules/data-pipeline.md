---
description: ETL pipeline — execution order, column normalization, data directory rules
globs:
  - data_processing/**
  - data/**
  - database/reset_database.py
  - tests/data_processing/**
---

# Data Pipeline

## Execution Order (each stage depends on previous)

1. `consolidate_sites.py` → Clean CSVs, create master_sites.csv
2. `site_processing.py` → Load sites into database
3. `merge_sites.py` → Deduplicate sites by Haversine clustering (50m threshold)
4. `chemical_processing.py` → Original chemical CSV data
5. `updated_chemical_processing.py` → Range-based chemical CSV data
6. `fish_processing.py` → Fish IBI scores (uses bt_fieldwork_validator)
7. `macro_processing.py` → Macroinvertebrate assessments
8. `habitat_processing.py` → Habitat assessments

Orchestrated by `database/reset_database.py`.

## Data Directory Rules

- `data/raw/` — **read-only**, never modify original CSVs
- `data/interim/` — cleaned CSVs (output of consolidate_sites phase 1)
- `data/processed/` — database-ready outputs and chatbot data exports

## Column Name Normalization

Applied at load time across all data types: lowercase, spaces/hyphens → underscores, special characters removed.

## Duplicate Handling by Type

- **Sites**: Haversine coordinate clustering (50m threshold, union-find grouping)
- **Chemical**: All records preserved (no dedup)
- **Fish**: BT field work records distinguish replicates from errors
- **Habitat**: Same-date duplicates averaged, grade recalculated
- **Macro**: Unique by (site, sample_id, habitat)

## Site Name Matching

- Strip whitespace, collapse multiple spaces
- Exact match for DB lookups
- Fuzzy fallback: 85% threshold (data_loader), 90% (bt_fieldwork_validator)
