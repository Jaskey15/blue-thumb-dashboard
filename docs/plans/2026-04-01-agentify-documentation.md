# Agentify Documentation Structure

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Restructure project documentation to match the agentic architecture principles in the root `~/.claude/CLAUDE.md` — slim CLAUDE.md to ~50 lines, create path-scoped `.claude/rules/` files, and add a Lessons Learned section.

**Architecture:** Extract actionable AI context (conventions, gotchas, integration details) from the bloated CLAUDE.md and `docs/architecture/` into `.claude/rules/` files with YAML frontmatter path globs. Keep `docs/` as-is for human reference. The slimmed CLAUDE.md retains only: tech stack, run commands, high-level architecture decisions, critical rules, and lessons learned.

**Principles:** Content in rules files should be concise and actionable — not a copy-paste of the full docs. Extract what Claude needs to know when touching files in that scope. The full docs remain in `docs/` for human deep-dives.

---

### Task 1: Create `.claude/rules/` directory

**Step 1:** Create the directory

```bash
mkdir -p .claude/rules
```

---

### Task 2: Create `.claude/rules/chemical.md`

Scoped to chemical data processing files. Extracted from CLAUDE.md gotchas, DATA_PIPELINE.md chemical sections, and DATABASE_SCHEMA.md chemical tables.

**File:** Create `.claude/rules/chemical.md`

```markdown
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
```

---

### Task 3: Create `.claude/rules/database.md`

Scoped to database files. Extracted from CLAUDE.md conventions, DATABASE_SCHEMA.md connection patterns and Cloud Run lifecycle.

**File:** Create `.claude/rules/database.md`

```markdown
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
```

---

### Task 4: Create `.claude/rules/dashboard.md`

Scoped to callbacks and layouts. Extracted from CLAUDE.md conventions and DASHBOARD.md patterns.

**File:** Create `.claude/rules/dashboard.md`

```markdown
---
description: Dashboard conventions — component IDs, callback patterns, state management
globs:
  - callbacks/**
  - layouts/**
  - visualizations/**
  - app.py
  - tests/**/test_*callback*
---

# Dashboard Conventions

## Component IDs

- **kebab-case**, prefixed by tab: `chemical-site-dropdown`, `bio-tab-state`, `habitat-grade-chart`
- State stores: `<tab>-tab-state`
- Dropdowns: `<tab>-site-dropdown`, `<tab>-parameter-dropdown`

## Callback Patterns

- All callbacks registered centrally: `callbacks/__init__.py` → `register_callbacks(app)` → each module's `register_<type>_callbacks(app)`
- Convention: `prevent_initial_call=True` on all callbacks
- Trigger identification via `callback_context`
- Cascading updates: site selection → parameter options → visualization
- Lazy loading: visualizations only render when tab is active

## State Management

Uses `dcc.Store` (session storage) for tab state persistence.

**Priority:** saved state > navigation state > defaults

- Tab state stores save user selections (site, parameter, filters)
- Navigation store coordinates cross-tab communication (e.g., map click → tab switch)

## Status Color Scheme

```
Chemical:  Normal=#1e8449, Caution=#ff9800, Poor=#e74c3c
Fish:      Excellent=#2e7d32, Good=#66bb6a, Fair=#ffca28, Poor=#f57c00, Very Poor=#c62828
Macro:     Non-impaired=#1e8449, Slightly=#7cb342, Moderately=#ff9800, Severely=#e74c3c
Habitat:   A=#2e7d32, B=#66bb6a, C=#ffca28, D=#f57c00, F=#c62828
```

Colors defined in `visualizations/visualization_utils.py`.
```

---

### Task 5: Create `.claude/rules/data-pipeline.md`

Scoped to data processing and data directories. Extracted from DATA_PIPELINE.md core sections.

**File:** Create `.claude/rules/data-pipeline.md`

```markdown
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
```

---

### Task 6: Create `.claude/rules/cloud.md`

Scoped to cloud function and deployment files. Extracted from DEPLOYMENT.md.

**File:** Create `.claude/rules/cloud.md`

```markdown
---
description: Cloud deployment — Cloud Run, Cloud Functions, environment detection, sync strategy
globs:
  - cloud_functions/**
  - config/**
  - Dockerfile
  - deploy.sh
---

# Cloud Infrastructure

## Environment Detection (`config/gcp_config.py`)

GCP detected via: `GOOGLE_CLOUD_PROJECT`, `GAE_APPLICATION`, or `K_SERVICE` env vars.

| Setting | Local | GCP |
|---------|-------|-----|
| DB path | `database/blue_thumb.db` | `/tmp/blue_thumb.db` |
| Log level | DEBUG | INFO |
| Debug mode | True | False |

## Cloud Run

- Docker image: `python:3.12-slim`, Gunicorn on port 8080
- CD: push to `main` → Cloud Build trigger → Docker build → Cloud Run deploy (us-central1)
- Vertex AI chatbot authenticates via service account, no API key needed
- Required env vars: `GOOGLE_CLOUD_PROJECT`, `GCS_BUCKET_DATABASE`

## Cloud Function (Data Sync)

Located in `cloud_functions/survey123_sync/`. The directory name is legacy — retained for GCP config compatibility.

**Sync strategy:**
1. First run (no prior sync metadata): fetch by sampling date from DB's latest chemical date
2. Subsequent runs: fetch by `EditDate` from last successful sync timestamp
3. Metadata stored at `sync_metadata/last_feature_server_sync.json` in GCS

**Deploy:** `cd cloud_functions/survey123_sync && ./deploy.sh`
- `deploy.sh` creates staging dir bundling function code + shared project modules
- Cloud Scheduler triggers daily at 6 AM Central

## Logging

`utils.setup_logging(module_name, category=...)` — in cloud envs, writes to `/tmp` instead of project root.
```

---

### Task 7: Slim down CLAUDE.md

**File:** Modify `CLAUDE.md`

Replace the entire file with a ~50 line version that keeps: tech stack, quick start, high-level architecture decisions, critical rules, task routing (trimmed), and a new Lessons Learned section. Remove: project structure tree, gotchas (moved to rules), detailed conventions (moved to rules), architecture docs table (those are for humans).

```markdown
# Blue Thumb Water Quality Dashboard

Interactive Dash/Plotly dashboard for Oklahoma's Blue Thumb volunteer stream monitoring program. Visualizes chemical, biological, and habitat data across 370+ sites with AI chatbot assistance and automated cloud data sync.

## Tech Stack

Python 3.12+ | Dash 3.0.3 | Plotly 6.0.1 | SQLite | Pandas | Bootstrap | Vertex AI Gemini 2.0 | Google Cloud (Cloud Run, Cloud Functions, Cloud Storage)

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m database.reset_database
python app.py    # http://127.0.0.1:8050
```

## Architecture Decisions

- **Three active chemical data pathways** — `chemical_processing.py` (original CSV), `updated_chemical_processing.py` (range-based CSV), and `arcgis_sync.py` (real-time FeatureServer). All share `chemical_utils.py`. These are NOT versions of each other — they handle different data formats.
- **GCS-backed database on Cloud Run** — DB downloaded from GCS at startup, background thread polls for updates. Docker-baked DB is fallback only.
- **`data_queries.py` is the primary retrieval interface** — callbacks and visualizations query through this module. Exception: `map_queries.py` for map-specific queries.

## Critical Rules

- Always use parameterized queries (`?`) — never string formatting
- Sites must exist before loading any other data type (FK constraints)
- `data/raw/` is read-only — cleaned versions go to `data/interim/`
- Column names normalized to lowercase with underscores at load time

## Common Task Routing

| Task | Key files |
|------|-----------|
| Add/change chemical parameter | `chemical_utils.py`, `db_schema.py`, `visualization_utils.py` |
| Add new dashboard tab | `layouts/tabs/`, `callbacks/`, `callbacks/__init__.py` |
| Modify map behavior | `visualizations/map_viz.py`, `map_queries.py`, `callbacks/overview_callbacks.py` |
| Update cloud sync | `cloud_functions/survey123_sync/main.py`, `data_processing/arcgis_sync.py` |

## Testing

```bash
pytest                          # Full suite
pytest tests/data_processing/   # By module
```

## Lessons Learned

_None yet. Add entries as `Problem → Rule` when mistakes happen._
```

---

### Task 8: Verify and commit

**Step 1:** Verify all rule files load correctly by checking YAML frontmatter syntax

```bash
head -10 .claude/rules/*.md
```

**Step 2:** Verify CLAUDE.md line count is under 50

```bash
wc -l CLAUDE.md
```

**Step 3:** Commit

```bash
git add .claude/rules/ CLAUDE.md docs/plans/2026-04-01-agentify-documentation.md
git commit -m "docs: restructure project docs for agentic architecture

- Slim CLAUDE.md from 85 to ~50 lines per root principles
- Create .claude/rules/ with 5 path-scoped rule files:
  chemical.md, database.md, dashboard.md, data-pipeline.md, cloud.md
- Extract conventions, gotchas, and integration details into rules
- Add Lessons Learned section to CLAUDE.md
- Keep docs/ as-is for human reference"
```
