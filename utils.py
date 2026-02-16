"""
This module contains reusable helper functions used across the dashboard.
"""

import os
import traceback
import pandas as pd

# Common style configurations
CAPTION_STYLE = {
    'font-style': 'italic',
    'color': '#666',
    'font-size': '0.9rem',
    'margin-top': '0.5rem',
    'text-align': 'center'
}

DEFAULT_IMAGE_STYLE = {
    'width': '100%',
    'max-width': '100%',
    'height': 'auto'
}

def setup_logging(module_name, category="general"):
    """
    Configure component-specific logging with organized directory structure.
    """
    import logging
    import os

    # Project root discovery
    def find_project_root():
        current_dir = os.getcwd()
        max_levels = 5
        
        for _ in range(max_levels):
            if os.path.exists(os.path.join(current_dir, 'app.py')):
                return current_dir
            
            parent_dir = os.path.dirname(current_dir)
            if parent_dir == current_dir:  # Reached system root
                break
            current_dir = parent_dir

        raise FileNotFoundError(
            f"Could not find project root (app.py) within {max_levels} parent directories. "
            f"Make sure app.py exists in your project root."
        )

    if (
        os.environ.get('K_SERVICE')
        or os.environ.get('K_REVISION')
        or os.environ.get('FUNCTION_TARGET')
        or os.environ.get('GAE_APPLICATION')
    ):
        project_root = '/tmp'
    else:
        project_root = find_project_root()

    logs_dir = os.path.join(project_root, 'logs', category)
    os.makedirs(logs_dir, exist_ok=True)

    log_file = os.path.join(logs_dir, f"{module_name}.log")
    
    # Module-specific logger configuration
    logger = logging.getLogger(module_name)
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent conflicts with root logger
    
    # Prevent propagation to root logger to avoid conflicts
    logger.propagate = False
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def round_parameter_value(param_name, value, data_type='chemical'):
    """
    Round values to parameter-appropriate precision for consistent display.
    """
    if pd.isna(value) or value is None:
        return None
    
    try:
        float_value = float(value)
        
        if data_type == 'chemical':
            if param_name == 'do_percent':
                return int(round(float_value))
            elif param_name == 'pH':
                return float(f"{float_value:.1f}")
            elif param_name == 'soluble_nitrogen':
                return float(f"{float_value:.2f}")
            elif param_name == 'Phosphorus':
                return float(f"{float_value:.3f}")
            elif param_name == 'Chloride':
                return int(round(float_value))
            else:
                return float(f"{float_value:.2f}")  # Default for unknown chemical parameters
                
        elif data_type == 'bio':
            return float(f"{float_value:.2f}")
            
        elif data_type == 'habitat':
            return int(round(float_value))
            
        else:
            return float(f"{float_value:.2f}")  # Default rounding
            
    except (ValueError, TypeError):
        logger.warning(f"Could not round value {value} for parameter {param_name}")
        return None

def load_markdown_content(filename, fallback_message=None, link_target=None):
    """
    Load and convert markdown files to Dash components with error handling.
    """
    logger = setup_logging("load_markdown_content", category="utils")

    try:
        from dash import dcc, html

        base_dir = os.path.dirname(__file__) 
        file_path = os.path.join(base_dir, 'text', filename)  

        if not os.path.exists(file_path):
            error_msg = f"Markdown file not found: {file_path}"
            logger.error(error_msg)
            return html.Div(
                fallback_message or f"Content not available: {filename}",
                className="alert alert-warning"
            )

        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            
        # Optional link targeting for external links
        markdown_props = {}
        if link_target:
            markdown_props['link_target'] = link_target
            
        return html.Div([
            dcc.Markdown(content, **markdown_props)
        ], className="markdown-content")
    
    except Exception as e:
        error_msg = f"Error loading content from {filename}: {e}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())

        try:
            from dash import html

            return html.Div(
                fallback_message or f"Error loading content: {str(e)}",
                className="alert alert-danger"
            )
        except Exception:
            return fallback_message or f"Error loading content: {str(e)}"
    
def get_sites_with_data(data_type):
    """
    Get sites that have actual data measurements for filtering purposes.
    """
    from database.database import close_connection, get_connection

    logger = setup_logging("get_sites_with_data", category="utils")
    
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Queries target sites with actual measurement data
        queries = {
            'chemical': """
                SELECT DISTINCT s.site_name 
                FROM sites s
                JOIN chemical_collection_events c ON s.site_id = c.site_id
                JOIN chemical_measurements m ON c.event_id = m.event_id
                ORDER BY s.site_name
            """,
            'fish': """
                SELECT DISTINCT s.site_name 
                FROM sites s
                JOIN fish_collection_events f ON s.site_id = f.site_id
                JOIN fish_summary_scores fs ON f.event_id = fs.event_id
                ORDER BY s.site_name
            """,
            'macro': """
                SELECT DISTINCT s.site_name 
                FROM sites s
                JOIN macro_collection_events m ON s.site_id = m.site_id
                JOIN macro_summary_scores ms ON m.event_id = ms.event_id
                ORDER BY s.site_name
            """,
            'habitat': """
                SELECT DISTINCT s.site_name 
                FROM sites s
                JOIN habitat_assessments h ON s.site_id = h.site_id
                JOIN habitat_summary_scores hs ON h.assessment_id = hs.assessment_id
                ORDER BY s.site_name
            """
        }
        
        if data_type not in queries:
            logger.error(f"Unknown data type: {data_type}")
            return []
        
        cursor.execute(queries[data_type])
        sites = [row[0] for row in cursor.fetchall()]
        
        logger.debug(f"Found {len(sites)} sites with {data_type} data")
        return sites
        
    except Exception as e:
        logger.error(f"Error getting sites with {data_type} data: {e}")
        return []
        
    finally:
        if conn:
            close_connection(conn)
    
def create_metrics_accordion(table_component, title, accordion_id):
    """
    Wrap metrics tables in collapsible accordion for better UX.
    """
    logger = setup_logging("create_metrics_accordion", category="utils")

    try:
        from dash import html
        import dash_bootstrap_components as dbc
        accordion = html.Div([
            dbc.Accordion([
                dbc.AccordionItem(
                    table_component,
                    title=title,
                ),
            ], start_collapsed=True, id=accordion_id)
        ])
        
        return accordion
    except Exception as e:
        logger.error(f"Error creating accordion {accordion_id}: {e}")

        try:
            from dash import html

            return html.Div(
                f"Could not create metrics table: {str(e)}",
                className="alert alert-warning"
            )
        except Exception:
            return f"Could not create metrics table: {str(e)}"

def create_image_with_caption(src, caption, className="img-fluid", style=None, alt_text=None):
    """
    Create accessible image components with consistent styling.
    """
    logger = setup_logging("create_image_with_caption", category="utils")

    try:
        from dash import html

        if style is None:
            style = DEFAULT_IMAGE_STYLE.copy()
            
        if alt_text is None:
            alt_text = caption  # Use caption as fallback for accessibility
        
        return html.Div([
            html.Img(
                src=src,
                className=className,
                style=style,
                alt=alt_text
            ),
            html.Figcaption(
                caption,
                style=CAPTION_STYLE
            )
        ], style={'width': '100%', 'margin-bottom': '1rem'})
    
    except Exception as e:
        logger.error(f"Error creating image with caption: {e}")

        try:
            from dash import html

            return html.Div(
                f"Image could not be loaded: {str(e)}",
                className="alert alert-warning"
            )
        except Exception:
            return f"Image could not be loaded: {str(e)}"

def safe_div(a, b, default=0):
    """
    Prevent division by zero errors in calculations.
    """
    try:
        return a / b if b != 0 else default
    except:
        return default

def format_value(value, precision=2, unit=None):
    """
    Standardize numerical display formatting across components.
    """
    try:
        if value is None:
            return "N/A"
        
        formatted = f"{float(value):.{precision}f}"
        if unit:
            formatted += f" {unit}"
        
        return formatted
    except:
        return "N/A"

def get_parameter_label(param_type, param_name):
    """Format parameter names for axis labels and titles."""
    from config.shared_constants import PARAMETER_AXIS_LABELS
    return PARAMETER_AXIS_LABELS.get(param_name, param_name.replace('_', ' ').title())

def get_parameter_name(parameter):
    """Convert parameter codes to human-readable display names."""
    from config.shared_constants import PARAMETER_DISPLAY_NAMES
    return PARAMETER_DISPLAY_NAMES.get(parameter, parameter)