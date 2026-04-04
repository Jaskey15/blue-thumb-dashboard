---
description: Database conventions — connection pattern, parameterized queries, FK ordering, Cloud Run lifecycle
globs:
  - database/**
  - data_processing/data_queries.py
  - tests/**/test_database*
  - tests/**/test_data_queries*
---

# Database Conventions

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

- Always use parameterized queries (`?` placeholders) — never string formatting
- Foreign keys enforced via `PRAGMA foreign_keys = ON` on every connection
- `execute_query(query, params)` wrapper available with rollback on error

## FK Ordering

Sites must exist before loading any data type — all processing tables have foreign keys to `sites`. Pipeline order matters: sites first, then chemical/fish/macro/habitat.

## Cloud Run DB Lifecycle

On Cloud Run (`K_SERVICE` env var detected):
1. First `get_connection()` downloads DB from GCS bucket to `/tmp`
2. Background daemon thread polls GCS blob generation every 300s
3. Per-request lightweight generation check (rate-limited)
4. Fallback to Docker-bundled `database/blue_thumb.db` if GCS fails

## Primary Query Interface

`data_processing/data_queries.py` is the primary retrieval interface for the dashboard. Callbacks and visualizations should query through this module. Exception: `visualizations/map_queries.py` has direct DB access for map-specific optimized queries.
