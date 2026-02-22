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

## Project Structure

```
app.py                          # Dash app entry point
config/                         # GCP config, shared constants
database/                       # Schema (db_schema.py), connections (database.py), reset pipeline
data_processing/                # ETL pipeline — see docs/architecture/DATA_PIPELINE.md
callbacks/                      # Dash callbacks — see docs/architecture/DASHBOARD.md
layouts/tabs/                   # Tab layout components (overview, chemical, biological, habitat, etc.)
layouts/components/chatbot.py   # Floating AI chatbot component
visualizations/                 # Plotly chart generation and map queries
cloud_functions/survey123_sync/ # Daily data sync Cloud Function
text/                           # Markdown content for educational tabs
data/{raw,interim,processed}/   # CSV pipeline stages (raw → interim → processed)
tests/                          # 51 test files mirroring source structure
```

## Key Conventions

- **Column names**: All normalized to lowercase with underscores at load time
- **Component IDs**: kebab-case, prefixed by tab (`chemical-site-dropdown`, `bio-tab-state`)
- **Database**: Always use parameterized queries (`?`). Foreign keys enforced. Connection pattern: `get_connection()` → try/finally → `close_connection()`
- **BDL handling**: Zero = below detection limit (replaced with threshold). NaN = data gap (preserved)
- **Callbacks**: All use `prevent_initial_call=True`. Registered centrally in `callbacks/__init__.py`
- **State**: `dcc.Store` for session persistence. Priority: saved state > navigation state > defaults
- **Three chemical data pathways**: `chemical_processing.py` (original single-value CSV), `updated_chemical_processing.py` (multi-range Low/Mid/High CSV), and `arcgis_sync.py` (real-time FeatureServer). All share `chemical_utils.py`

## Gotchas

- **`updated_chemical_processing.py` is NOT a newer version of `chemical_processing.py`** — they handle different data formats from different collection periods. Both are active. `arcgis_sync.py` is a third pathway that fetches from the FeatureServer and feeds into the same pipeline as `updated_chemical_processing.py`.
- **Sites must exist before loading any data type** — all processing tables have foreign keys to `sites`. Run site pipeline first.
- **`data/raw/` is read-only** — never modify raw CSVs. Cleaned versions go to `data/interim/`.
- **`data_queries.py` is the only retrieval interface** — callbacks and visualizations should never query the DB directly. All reads go through this module.
- **Duplicate handling differs by data type** — chemical preserves all records, habitat averages same-date duplicates, fish uses BT field work records to distinguish replicates from errors. See `docs/decisions/` for rationale.
- **Soluble nitrogen is calculated, not measured** — it's the sum of Nitrate + Nitrite + Ammonia, computed during chemical processing.

## Common Task Routing

| Task | Key files to modify |
|------|-------------------|
| Add/change a chemical parameter | `chemical_utils.py` (constants), `db_schema.py` (schema), `visualization_utils.py` (colors) |
| Add a new dashboard tab | `layouts/tabs/` (layout), `callbacks/` (new callback module), `callbacks/__init__.py` (register) |
| Change how data is queried for display | `data_processing/data_queries.py` |
| Modify map behavior | `visualizations/map_viz.py`, `visualizations/map_queries.py`, `callbacks/overview_callbacks.py` |
| Update cloud sync logic | `cloud_functions/survey123_sync/main.py`, `chemical_processor.py`, `data_processing/arcgis_sync.py` |
| Change status thresholds | `db_schema.py` (CHEMICAL_REFERENCE_VALUES constant) |

## Testing

```bash
pytest                          # Full suite
pytest tests/data_processing/   # By module
pytest -m unit                  # By marker (unit, integration, slow)
```

See `docs/testing/TESTING_GUIDE.md` for full details.

## Architecture Docs

| Doc | What it covers |
|-----|---------------|
| `docs/architecture/DATA_PIPELINE.md` | ETL execution order, file roles, shared conventions, domain glossary |
| `docs/architecture/DATABASE_SCHEMA.md` | Table relationships, indexes, connection patterns |
| `docs/architecture/DASHBOARD.md` | Callbacks, state management, visualization flow, chatbot integration |
| `docs/testing/TESTING_GUIDE.md` | Running tests, organization, fixtures, adding new tests |
| `docs/cloud/DEPLOYMENT.md` | Docker, Cloud Build, Cloud Functions, env vars |
| `docs/decisions/` | Historical design decisions (duplicate handling, site processing, validation) |
