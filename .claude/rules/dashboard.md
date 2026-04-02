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
