# Database Schema Reference

SQLite database at `database/blue_thumb.db`. Foreign keys enforced via `PRAGMA foreign_keys = ON` on every connection.

## Table Relationships

```
sites (site_id PK)
  ├── chemical_collection_events (site_id FK)
  │     └── chemical_measurements (event_id FK, parameter_id FK)
  ├── fish_collection_events (site_id FK)
  │     ├── fish_metrics (event_id FK)
  │     └── fish_summary_scores (event_id FK)
  ├── macro_collection_events (site_id FK)
  │     ├── macro_metrics (event_id FK)
  │     └── macro_summary_scores (event_id FK)
  └── habitat_assessments (site_id FK)
        ├── habitat_metrics (assessment_id FK)
        └── habitat_summary_scores (assessment_id FK)

chemical_parameters (parameter_id PK)
  ├── chemical_reference_values (parameter_id FK)
  └── chemical_measurements (parameter_id FK)
```

## Core Tables

### sites
| Column | Type | Notes |
|--------|------|-------|
| site_id | INTEGER PK | Auto-increment |
| site_name | TEXT UNIQUE | Primary lookup key |
| latitude | REAL | |
| longitude | REAL | |
| county | TEXT | |
| river_basin | TEXT | |
| ecoregion | TEXT | |
| active | BOOLEAN | 1=active, 0=historic |
| last_chemical_reading_date | TEXT | Used for active classification |

### chemical_collection_events
| Column | Type | Notes |
|--------|------|-------|
| event_id | INTEGER PK | Auto-increment |
| site_id | INTEGER FK | References sites |
| sample_id | INTEGER | Optional |
| collection_date | TEXT | YYYY-MM-DD format |
| year | INTEGER | |
| month | INTEGER | |

Duplicates allowed (same site + date) to preserve replicate samples.

### chemical_measurements
| Column | Type | Notes |
|--------|------|-------|
| event_id | INTEGER FK | Composite PK with parameter_id |
| parameter_id | INTEGER FK | References chemical_parameters (1-5) |
| value | REAL | |
| bdl_flag | BOOLEAN | Default 0 |
| status | TEXT | Normal/Caution/Poor |

### fish_collection_events
| Column | Type | Notes |
|--------|------|-------|
| event_id | INTEGER PK | |
| site_id | INTEGER FK | |
| sample_id | INTEGER | UNIQUE with site_id |
| collection_date | TEXT | |
| year | INTEGER | |

### fish_metrics / fish_summary_scores
- **fish_metrics**: (event_id, metric_name) PK — stores raw_value, metric_result, metric_score
- **fish_summary_scores**: total_score, comparison_to_reference (%), integrity_class (Excellent/Good/Fair/Poor/Very Poor)

### macro_collection_events
| Column | Type | Notes |
|--------|------|-------|
| event_id | INTEGER PK | |
| site_id | INTEGER FK | |
| sample_id | INTEGER | UNIQUE with (site_id, habitat) |
| collection_date | TEXT | |
| season | TEXT | CHECK: Summer or Winter |
| year | INTEGER | |
| habitat | TEXT | CHECK: Riffle, Vegetation, or Woody |

### macro_metrics / macro_summary_scores
- **macro_metrics**: (event_id, metric_name) UNIQUE — raw_value, metric_score
- **macro_summary_scores**: total_score, comparison_to_reference (%), biological_condition

### habitat_assessments
| Column | Type | Notes |
|--------|------|-------|
| assessment_id | INTEGER PK | |
| site_id | INTEGER FK | |
| assessment_date | TEXT | |
| year | INTEGER | |

### habitat_metrics / habitat_summary_scores
- **habitat_metrics**: (assessment_id, metric_name) PK — score (0-100 per metric)
- **habitat_summary_scores**: total_score (0-100), habitat_grade (A/B/C/D/F)

## Reference Data

### chemical_parameters (5 rows, static)
| ID | Code | Display Name | Unit |
|----|------|-------------|------|
| 1 | do_percent | Dissolved Oxygen | % |
| 2 | pH | pH | pH units |
| 3 | soluble_nitrogen | Nitrogen | mg/L |
| 4 | Phosphorus | Phosphorus | mg/L |
| 5 | Chloride | Chloride | mg/L |

### chemical_reference_values (12 rows, static)
Thresholds for status determination. Populated by `db_schema.py` at table creation.

## Indexes

```sql
idx_chemical_site_date    ON chemical_collection_events(site_id, collection_date)
idx_chemical_measurements ON chemical_measurements(event_id, parameter_id)
idx_macro_site_season     ON macro_collection_events(site_id, season, year)
idx_fish_site_year        ON fish_collection_events(site_id, year)
idx_habitat_site_year     ON habitat_assessments(site_id, year)
```

## Connection Pattern

```python
from database.database import get_connection, close_connection

conn = get_connection()
try:
    cursor = conn.cursor()
    cursor.execute("SELECT ...", params)
    conn.commit()
finally:
    close_connection(conn)
```

- `get_connection()` → creates connection, enables foreign keys
- `close_connection(conn)` → commits and closes
- `execute_query(query, params)` → wrapper with rollback on error
- Always use parameterized queries (`?` placeholders) — never string formatting
