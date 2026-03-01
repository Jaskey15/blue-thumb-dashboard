# Testing Guide

## Running Tests

```bash
# Full suite
pytest

# By directory
pytest tests/data_processing/
pytest tests/callbacks/
pytest tests/visualizations/

# By marker (defined in pytest.ini but not yet applied to tests)
# pytest -m unit
# pytest -m integration
# pytest -m "not slow"

# Single file
pytest tests/data_processing/test_chemical_processing.py

# Verbose with short traceback (default via pytest.ini)
pytest -v --tb=short
```

## Configuration

From `pytest.ini`:
```ini
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v --tb=short --strict-markers
markers =
    unit: Unit tests
    integration: Integration tests
    slow: Slow running tests
```

## Test Organization

Tests mirror the source directory structure:

```
tests/
├── app/                    → App initialization and configuration
│   ├── test_app_initialization.py
│   └── test_configuration.py
├── callbacks/              → Callback logic (largest test area)
│   ├── conftest.py         → Shared fixtures for callback tests
│   ├── test_biological_callbacks.py
│   ├── test_callback_decorators.py
│   ├── test_callback_utils.py
│   ├── test_chemical_callbacks.py
│   ├── test_habitat_callbacks.py
│   ├── test_overview_callbacks.py
│   └── test_shared_callbacks.py
├── data_processing/        → ETL pipeline logic
│   ├── test_biological_utils.py
│   ├── test_chemical_processing.py
│   ├── test_data_loader.py
│   ├── test_data_queries.py
│   ├── test_fish_processing.py
│   ├── test_habitat_processing.py
│   ├── test_macro_processing.py
│   ├── test_replicate_detection.py
│   ├── test_score_averaging.py
│   ├── test_site_consolidation.py
│   └── test_site_management.py
├── database/               → Database operations
│   ├── conftest.py         → DB fixtures (in-memory SQLite)
│   ├── test_connection.py
│   ├── test_operations.py
│   ├── test_reset.py
│   └── test_schema.py
├── integration/            → End-to-end workflows
│   ├── test_data_pipeline.py
│   └── test_navigation_flows.py
├── layouts/                → Layout component rendering
│   ├── test_layout_helpers.py
│   └── test_tabs.py
├── survey123_sync/         → Cloud function tests
│   ├── test_arcgis_auth.py
│   ├── test_chemical_processor.py
│   ├── test_data_processing.py  → Includes TestSyncModeBehavior (FeatureServer sync mode, routing, metadata)
│   ├── test_database_manager.py
│   └── test_survey123_fetcher.py
├── visualizations/         → Chart generation
│   ├── test_chemical_viz.py
│   ├── test_fish_viz.py
│   ├── test_habitat_viz.py
│   ├── test_macro_viz.py
│   ├── test_map_viz.py
│   └── test_visualization_utils.py
└── test_utils.py           → Utility function tests
```

**40 test files** across 8 test directories (796 tests total).

## Testing Philosophy

- **Logic testing**: Test core processing functions directly — no Dash server needed
- **Component testing**: Test individual callback pieces and their outputs
- **Integration testing**: Test workflows combining multiple processing steps
- **Error handling**: Cover edge cases, malformed data, missing values

### What tests validate
- State persistence and restoration across tab switches
- Dropdown population and sorting logic
- Navigation routing between tabs
- Error state creation and graceful degradation
- Data query correctness and formatting
- Visualization output structure
- Database schema integrity and constraint enforcement
- ArcGIS FeatureServer field translation and site name normalization
- `sample_id`-based idempotent chemical event insertion
- Cloud Function sync mode selection and precedence
- FeatureServer sync pipeline (fetch, process, upload, metadata tracking)

## Key Fixtures

- **`tests/database/conftest.py`**: In-memory SQLite database with schema for isolated DB tests
- **`tests/callbacks/conftest.py`**: Mock data and state objects for callback testing
- Standard `pytest-mock` for mocking database connections and external APIs

## Adding New Tests

1. Create test file in the matching `tests/<module>/` directory
2. Name it `test_<module_name>.py`
3. Use `Test*` classes for grouping related tests
4. Apply markers (`@pytest.mark.unit`, `@pytest.mark.integration`) as appropriate — markers are defined in `pytest.ini` but not yet applied to existing tests
5. Use existing conftest fixtures for database and callback state
