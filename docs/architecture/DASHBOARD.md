# Dashboard Architecture

Dash/Plotly web application with tab-based navigation, session state management, and AI chatbot integration.

## Application Flow

```
app.py (entry point)
  ├── Initializes Dash app with Bootstrap Sandstone theme
  ├── Defines header, tab bar, footer, modals
  ├── Creates dcc.Store components for state persistence
  └── Calls register_callbacks(app) from callbacks/__init__.py
```

Server exposed as `app.server` for Gunicorn deployment. Port 8050 (local) or 8080 (Cloud Run).

## Tab System

| Tab | Layout File | Callback File | Data Source |
|-----|------------|---------------|-------------|
| Overview | `layouts/tabs/overview.py` | `callbacks/overview_callbacks.py` | `visualizations/map_queries.py` |
| Chemical | `layouts/tabs/chemical.py` | `callbacks/chemical_callbacks.py` | `data_queries.get_chemical_data_from_db()` |
| Biological | `layouts/tabs/biological.py` | `callbacks/biological_callbacks.py` | `data_queries.get_fish_dataframe()`, `get_macroinvertebrate_dataframe()` |
| Habitat | `layouts/tabs/habitat.py` | `callbacks/habitat_callbacks.py` | `data_queries.get_habitat_dataframe()` |
| Protect Our Streams | `layouts/tabs/protect_streams.py` | — (static content) | Markdown files in `text/` |
| Source Data | `layouts/tabs/source_data.py` | — | Raw data display |

Each layout function: `create_<tab>_tab()` returns a Dash component tree.

## Callback Architecture

### Registration
All callbacks registered centrally via `callbacks/__init__.py` → `register_callbacks(app)`, which imports and calls each module's `register_<type>_callbacks(app)`.

### Shared Modules
- **`shared_callbacks.py`** — Cross-tab navigation routing, modal toggles, parameter detection from map clicks
- **`tab_utilities.py`** — Legend creation, visualization formatting, shared UI generation
- **`helper_functions.py`** — `create_error_state()`, `create_empty_state()` factories

### State Management
Uses `dcc.Store` (session storage) for tab state persistence:

```python
@app.callback(
    Output('chemical-tab-state', 'data'),
    [Input('chemical-site-dropdown', 'value'), ...],
    [State('chemical-tab-state', 'data')],
    prevent_initial_call=True
)
```

- **Tab state stores** save user selections (selected site, parameter, filters)
- **Navigation store** coordinates cross-tab communication (map click → tab switch)
- State priority: saved state > navigation state > defaults

### Callback Patterns
1. **Trigger identification**: `callback_context` determines which input fired
2. **Cascading updates**: site selection → parameter options → visualization
3. **Lazy loading**: visualizations only render when tab is active
4. **`prevent_initial_call=True`**: standard on all callbacks to avoid unnecessary execution

## Component ID Conventions

- **kebab-case** for all IDs: `site-map-graph`, `chemical-tab-state`, `bio-site-dropdown`
- Prefixed by tab/feature: `chemical-`, `bio-`, `habitat-`, `overview-`
- State stores: `<tab>-tab-state`
- Dropdowns: `<tab>-site-dropdown`, `<tab>-parameter-dropdown`

## Visualization Module

```
visualizations/
├── map_viz.py              → create_basic_site_map(), add_parameter_colors_to_map()
├── map_queries.py          → Optimized queries for map data (latest readings per site)
├── chemical_viz.py         → create_time_series_plot(), create_all_parameters_view()
├── fish_viz.py             → create_fish_viz(), fish metrics accordion
├── macro_viz.py            → create_macro_viz(), macro metrics accordion
├── habitat_viz.py          → create_habitat_viz(), habitat metrics accordion
└── visualization_utils.py  → Shared colors, hover text formatting, trace creation
```

### Status Color Scheme
```
Chemical:  Normal=#1e8449 (green), Caution=#ff9800 (orange), Poor=#e74c3c (red)
Fish:      Excellent=#2e7d32, Good=#66bb6a, Fair=#ffca28, Poor=#f57c00, Very Poor=#c62828
Macro:     Non-impaired=#1e8449, Slightly=#7cb342, Moderately=#ff9800, Severely=#e74c3c
Habitat:   A=#2e7d32, B=#66bb6a, C=#ffca28, D=#f57c00, F=#c62828
```

## Chatbot Integration

- **AI model**: Vertex AI Gemini 2.0 via `google-genai` SDK
- **Callback**: `callbacks/chatbot_callbacks.py` → `register_chatbot_callbacks(app)`
- **UI component**: `layouts/components/chatbot.py` → `create_floating_chatbot(tab_name)`
- **Knowledge base**: Preprocessed files in `data/processed/chatbot_data/`
- **Tab-aware**: Current tab name passed as context for relevant responses
- **Conversation history**: Maintained in session via dcc.Store

## Layout Helpers

- `layouts/helpers.py` — Species gallery generation, UI utilities
- `layouts/constants.py` — `PARAMETER_OPTIONS` dropdown definitions
- `layouts/ui_data.py` — Static UI content (diagram captions, species metadata)
- `layouts/modals.py` — Icon attribution and image credits modals

## Static Content

```
text/
├── monitoring_sites.md
├── chemical/          → Parameter explanations (DO, pH, Nitrogen, Phosphorus, Chloride)
├── biological/        → Fish and macro community guides
├── habitat_analysis.md
└── protect_our_streams_intro.md
```

Loaded via markdown readers in layout files. Used for educational content in tabs and chatbot grounding.

## Config

- `config/gcp_config.py` — Environment detection (local vs GCP), asset URLs, DB paths, log levels
- `config/shared_constants.py` — `PARAMETER_DISPLAY_NAMES`, `PARAMETER_AXIS_LABELS`, `SEASON_MONTHS`
- `database/database.py` — On Cloud Run, manages GCS-backed database with background refresh thread and per-request generation checks (see `docs/architecture/DATABASE_SCHEMA.md` for details)
