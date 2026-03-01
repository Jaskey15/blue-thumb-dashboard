# Comment Style Guide

This guide establishes consistent commenting standards for the Blue Thumb Dashboard codebase, based on industry best practices and patterns refined during the codebase improvement process.

## Core Principles

### 1. Comments Should Explain WHY, Not WHAT
- **Good**: `# Preserve user selections between tab switches` (explains purpose)
- **Bad**: `# Update the state dictionary` (restates code)
- **Good**: `# Show basic map when no parameter selected` (explains logic flow)
- **Bad**: `# Create empty figure` (obvious from code)

### 2. Add Context Where Code Isn't Self-Explanatory
- **Good**: `# Restore parameter-specific view if previously selected`
- **Bad**: `# Get parameter from state` (obvious from code)
- **Good**: `# Filter out 'No data' items to keep legend clean`
- **Bad**: `# Filter legend items` (too vague)

### 3. Keep Comments Concise and Focused
- **Good**: `# Map initialization`
- **Bad**: `# ===== MAP INITIALIZATION AND SETUP ===== Initialize the map with basic settings and configure initial state`

## Comment Categories

### A. Module Docstrings
**Standard Format:**
```python
"""
[Core purpose] for the [system/component].
"""
```

**Examples:**
```python
# Good:
"""
Interactive map visualization and parameter exploration callbacks.
"""

# Bad:
"""
Overview callbacks for the Blue Thumb Stream Health Dashboard.
This file contains callbacks specific to the overview tab.
"""
```

### B. Function Docstrings
**For Complex Functions** - Include detailed behavior:
```python
def load_basic_map_on_tab_open(active_tab, overview_state):
    """
    Initialize map visualization with saved state.
    
    Priority order:
    1. Restore saved parameter and filtering
    2. Show basic site map with saved filtering
    3. Default to unfiltered view
    """
```

**For Simple Functions** - Keep it brief:
```python
def save_overview_tab_state(parameter_value, active_sites_toggle, current_state):
    """Preserve user selections between tab switches."""
```

### C. Section Comments
**Use Simple, Action-Oriented Headers:**
```python
# Good Examples:
# State persistence
# Map initialization
# Parameter visualization

# Bad Examples:
# ===========================================================================================
# 1. STATE MANAGEMENT
# ===========================================================================================
```

### D. Inline Comments
**Focus on Business Logic and Edge Cases:**
```python
# Good Examples:
if active_only_toggle:
    total_sites_count = get_total_site_count(active_only=False)  # Get full count for comparison
    
legend_items = [item for item in legend_items if "No data" not in item["label"]]  # Keep legend clean

# Bad Examples:
total_sites_count = get_total_site_count()  # Get the total count
legend_items.append(item)  # Add item to list
```

## Specific Guidelines by File Type

### Callback Files (`callbacks/`)
**State Management:**
```python
# Good:
def save_tab_state(value, current_state):
    """Preserve user selections between tab switches."""
    try:
        updated_state = current_state.copy() if current_state else {}
        updated_state['selected_value'] = value  # Only update if value changes
        return updated_state
    except Exception as e:
        logger.error(f"Error saving tab state: {e}")
        return current_state or {}  # Fallback to empty state if needed

# Bad:
def save_tab_state(value, current_state):
    """Save the tab state."""
    # Copy the current state
    updated_state = current_state.copy()
    # Update the value
    updated_state['selected_value'] = value
    # Return the updated state
    return updated_state
```

**Visualization Logic:**
```python
# Good:
def update_map_with_parameter(parameter_value, active_only):
    """
    Update map visualization based on parameter selection and filtering.
    
    Shows:
    - Parameter-specific coloring when parameter selected
    - Basic site map when no parameter selected
    - Filtered view based on active/historic toggle
    """

# Bad:
def update_map_with_parameter(parameter_value, active_only):
    """
    Update the map when a parameter is selected.
    This function updates the map colors and legend.
    It also handles the active sites toggle.
    """
```

**Error Handling:**
```python
# Good:
except Exception as e:
    logger.error(f"Error loading basic map: {e}")
    
    # Return safe fallback state for error recovery
    empty_map = {
        'data': [],
        'layout': {'title': 'Error loading map'}
    }
    return empty_map, True, None, False

# Bad:
except Exception as e:
    # Log the error
    logger.error(f"Error: {e}")
    # Return empty map
    return {}, True, None, False
```

### Layout Files (`layouts/`)
**Component Organization:**
```python
# Good:
# Parameter selection controls
html.Div([
    dcc.Dropdown(id="parameter-select"),
    dcc.Checklist(id="active-sites-toggle")
])

# Bad:
# Create a div for the parameter dropdown and checklist components
html.Div([
    # Dropdown for parameter selection
    dcc.Dropdown(id="parameter-select"),
    # Checklist for active sites toggle
    dcc.Checklist(id="active-sites-toggle")
])
```

### Visualization Files (`visualizations/`)
**Data Processing Logic:**
```python
# Good:
def add_parameter_colors(fig, param_type, param_name):
    """Add parameter-specific coloring to map markers."""
    # Use percentile ranges for chemical parameters
    if param_type == 'chemical':
        ranges = calculate_percentile_ranges(param_name)  # Dynamic thresholds
    else:
        ranges = STATIC_PARAMETER_RANGES[param_type]  # Pre-defined ranges
        
# Bad:
def add_parameter_colors(fig, param_type, param_name):
    """Add colors to the map."""
    # Calculate ranges
    if param_type == 'chemical':
        ranges = calculate_percentile_ranges(param_name)
    else:
        ranges = STATIC_PARAMETER_RANGES[param_type]
```

## Implementation Checklist

### Remove:
- [ ] Decorative comment blocks (`====`)
- [ ] Comments that just restate the code
- [ ] Obvious parameter descriptions
- [ ] Implementation details that don't add value

### Keep:
- [ ] Business logic explanations
- [ ] State management rationale
- [ ] Error handling strategies
- [ ] Complex data processing logic
- [ ] Non-obvious default values

### Add:
- [ ] Priority order in complex functions
- [ ] Edge case handling explanations
- [ ] State restoration logic
- [ ] Error recovery strategies

## Quality Metrics

A well-commented file should:
- Focus on **WHY** over **WHAT**
- Have clear section organization
- Explain complex business logic
- Document error handling strategies
- Use consistent terminology

## Review Process

When reviewing comments:
1. **Start with the code** - Is it self-documenting?
2. **Identify complexity** - Where do readers need help?
3. **Look for patterns** - Are similar components documented consistently?
4. **Check completeness** - Are all edge cases explained?
5. **Verify value** - Does each comment add understanding?

## Examples from Our Codebase

### Before and After - Overview Callbacks

**Before:**
```python
"""
Overview callbacks for the Blue Thumb Stream Health Dashboard.
This file contains callbacks specific to the overview tab.
"""

# ===========================================================================================
# 1. STATE MANAGEMENT
# ===========================================================================================

@app.callback(...)
def save_overview_tab_state(parameter_value, active_sites_toggle, current_state):
    """
    Save the current state of overview tab controls to session storage.
    This preserves user selections when they switch tabs and return.
    """
    # Update the state with current values
    updated_state = current_state.copy() if current_state else {}
```

**After:**
```python
"""
Interactive map visualization and parameter exploration callbacks.
"""

# State persistence
@app.callback(...)
def save_overview_tab_state(parameter_value, active_sites_toggle, current_state):
    """Preserve user selections between tab switches."""
    try:
        updated_state = current_state.copy() if current_state else {}
```

---

This style guide should be applied consistently across all modules to maintain clean, professional, and maintainable code documentation. 