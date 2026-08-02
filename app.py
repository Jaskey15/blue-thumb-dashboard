"""
Main application entry point and layout definition.

Configures core app settings and builds responsive layout structure
with tab-based navigation and state management.
"""

import dash
import dash_bootstrap_components as dbc
from dotenv import load_dotenv

from callbacks import register_callbacks
from dash import html, dcc
from layouts.tabs.overview import create_overview_tab
from layouts.tabs.chemical import create_chemical_tab
from layouts.tabs.biological import create_biological_tab
from layouts.tabs.habitat import create_habitat_tab
from layouts.tabs.protect_streams import create_protect_our_streams_tab
from layouts.tabs.source_data import create_source_data_tab
from layouts.modals import create_icon_attribution_modal, create_image_credits_modal

# Load environment variables from .env file
load_dotenv()

# Core app configuration
app = dash.Dash(__name__, 
                external_stylesheets=[
                    dbc.themes.SANDSTONE,
                    "https://use.fontawesome.com/releases/v5.15.4/css/all.css"
                ],
                suppress_callback_exceptions=True,
                meta_tags=[
                    {"name": "viewport", "content": "width=device-width, initial-scale=1, shrink-to-fit=no"}
                ])
server = app.server


@server.route("/health")
def health():
    """Keepalive target for Cloud Scheduler.

    Deliberately cheap: no database access, no layout rendering. Pinging this
    keeps an instance resident so visitors avoid the ~5s cold start, without
    the cost of running min-instances=1.
    """
    return {"status": "ok"}, 200


# Responsive header with background image overlay
header = dbc.Container([
    dbc.Row([
        dbc.Col(
            html.Div([
                html.H1("Blue Thumb Stream Health Dashboard", 
                       className="text-white text-center p-3",
                       style={
                           'font-size': 'clamp(1.5rem, 4vw, 2.5rem)',  # Scale font with viewport
                           'font-family': 'Montserrat, sans-serif',  
                           'font-weight': '700', 
                           'text-shadow': '2px 2px 4px rgba(0, 0, 0, 0.5)', 
                           'letter-spacing': '0.5px' 
                       })
            ], 
            style={
                'background-image': 'linear-gradient(rgba(0, 0, 0, 0.5), rgba(0, 0, 0, 0.5)), url("/assets/images/header_image.jpg")',  
                'background-size': 'cover',
                'background-position': 'center',
                'border-radius': '5px',
                'height': '200px',
                'min-height': '150px',
                'display': 'flex',
                'align-items': 'center',
                'justify-content': 'center'
            }),
            width=12
        )
    ])
])

# Main application layout
app.layout = dbc.Container([
    header,
    
    # Tab-based navigation structure
    dbc.Tabs([
        dbc.Tab(label="Overview", children=create_overview_tab(), tab_id="overview-tab"),
        dbc.Tab(label="Chemical Data", children=create_chemical_tab(), tab_id="chemical-tab"),
        dbc.Tab(label="Biological Data", children=create_biological_tab(), tab_id="biological-tab"),
        dbc.Tab(label="Habitat Data", children=create_habitat_tab(), tab_id="habitat-tab"),
        dbc.Tab(label="Protect Our Streams", children=create_protect_our_streams_tab(), tab_id="protect-tab"),
        dbc.Tab(label="Source Data", children=create_source_data_tab(), tab_id="source-tab"),
    ], id="main-tabs", active_tab="overview-tab"),

    # State management containers
    html.Div([
        # Enable cross-tab navigation from map interactions
        dcc.Store(id='navigation-store', storage_type='memory', data={'target_tab': None, 'target_site': None}),
        
        # Preserve user selections across sessions
        dcc.Store(id='overview-tab-state', storage_type='session', data={'selected_parameter': None, 'active_sites_only': False}),
        dcc.Store(id='habitat-tab-state', storage_type='session', data={'selected_site': None}),
        dcc.Store(id='biological-tab-state', storage_type='session', data={'selected_community': None, 'selected_site': None}),
        dcc.Store(id='chemical-tab-state', storage_type='session', data={
            'selected_site': None,
            'selected_parameter': None,
            'year_range': None,
            'selected_months': None,
            'highlight_thresholds': None
        })
    ], style={'display': 'none'}),

    # Responsive footer with attribution links
    dbc.Row([
        dbc.Col([
            html.Div([
                html.Img(
                    src="/assets/images/blue_thumb_logo.png",
                    height="60px",
                    style={"margin-right": "15px", "vertical-align": "middle"}
                ),
                html.Span([
                    "Data source: Blue Thumb Volunteer Monitoring Program | ",
                    "Map data © Esri | ",  
                    html.A("Icons from Flaticon", href="#", id="attribution-link"), " | ",
                    html.A("Image Credits", href="#", id="image-credits-link")
                ], style={"display": "inline-block", "vertical-align": "middle"})
            ], className="text-center mt-4 text-muted d-flex align-items-center justify-content-center mobile-footer-stack")
        ], width=12)
    ]),

    create_icon_attribution_modal(),
    create_image_credits_modal()
    
], fluid=True, className="px-4", style={"max-width": "1200px", "margin": "0 auto"})

# Initialize application callbacks
register_callbacks(app)

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 8050))  # Fallback to default port if not specified
    app.run(host='0.0.0.0', port=port, debug=False)