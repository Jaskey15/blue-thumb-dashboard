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

- **Two active chemical data pathways** — `chemical_processing.py` (legacy single-value CSV for pre-2020 data) and `arcgis_sync.py` (API-first pipeline fetching from ArcGIS Feature Server for current-period data). Both share `chemical_utils.py`. These are NOT versions of each other — different data formats, different collection periods.
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
| Update cloud sync | `cloud_functions/data_sync/main.py`, `data_processing/arcgis_sync.py` |

## Testing

```bash
pytest                          # Full suite
pytest tests/data_processing/   # By module
```

## Lessons Learned

_None yet. Add entries as `Problem → Rule` when mistakes happen._
