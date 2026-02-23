"""
Chemical tab layout for the dashboard
"""

import dash_bootstrap_components as dbc
from dash import dcc, html

from layouts.components.chatbot import create_floating_chatbot


def create_chemical_tab():
    """Create the chemical data tab layout with searchable dropdown for site selection."""

    return html.Div([
        dcc.Download(id="chemical-download-component"),

        # Add chatbot
        create_floating_chatbot("chemical"),

        # Description section
        html.Div([
            html.H3("Chemical Water Quality", className="mb-3"),
            html.P([
                "Chemical water quality data provides crucial insights into stream health by revealing "
                "pollution sources and environmental stressors that may not be immediately visible. ",
                "While chemical data alone cannot determine if a stream is healthy, monitoring key "
                "parameters helps detect problems and track restoration progress over time. ",
                "For more information on chemical testing procedures visit the ",
                html.A("Blue Thumb website", href="https://www.bluethumbok.com/monitoring-info.html", target="_blank", style={"text-decoration": "underline"}),
                ". Select a site and parameter below to begin analysis. You can find site names and "
                "locations on the ",
                html.A("overview tab", id="chemical-overview-link", href="#", style={"text-decoration": "underline"}),
                "."
            ])
        ], className="mb-4"),
        
        # Site selection
        html.Div([
            html.Label("Select Site:", className="form-label", style={'fontWeight': 'bold', 'fontSize': '1rem', 'marginBottom': '0.1rem'}),
            
            html.Small(
                "Click the dropdown and start typing to search for monitoring sites",
                className="text-muted mb-1 d-block"
            ),

            dcc.Dropdown(
                id='chemical-site-dropdown',
                options=[],  # Populated when tab loads
                placeholder="Search for a site...",
                searchable=True,
                clearable=True,
                className="mb-3"
            )
            
        ], style={'marginBottom': '20px'}),
        
        # Controls - hidden until site is selected
        html.Div([
            # Parameter selection
            dbc.Row([
                dbc.Col([
                    html.Label("Select Chemical Parameter:", className="form-label mb-2", style={'fontWeight': 'bold', 'fontSize': '1rem'}),
                    dcc.Dropdown(
                        id='chemical-parameter-dropdown',
                        options=[
                            {'label': 'Dissolved Oxygen', 'value': 'do_percent'},
                            {'label': 'pH', 'value': 'pH'},
                            {'label': 'Nitrogen', 'value': 'soluble_nitrogen'},
                            {'label': 'Phosphorus', 'value': 'Phosphorus'},
                            {'label': 'Chloride', 'value': 'Chloride'},
                            {'label': 'All Parameters', 'value': 'all_parameters'}
                        ],
                        value=None,
                        className="mb-3"
                    )
                ], width=12)
            ]),
            
            # Year range selection
            dbc.Row([
                dbc.Col([
                    html.Label(
                        "Start Year:", 
                        className="form-label mb-2",
                        style={"display": "inline-block", "vertical-align": "middle", "margin-right": "10px", "fontWeight": "bold", "fontSize": "1rem"}
                    ),
                    dcc.Dropdown(
                        id='start-year-dropdown',
                        options=[],
                        value=None,
                        clearable=False,
                        style={"display": "inline-block", "vertical-align": "middle", "width": "120px"}
                    )
                ], width=5),
                dbc.Col([
                    html.Label(
                        "End Year:", 
                        className="form-label mb-2",
                        style={"display": "inline-block", "vertical-align": "middle", "margin-right": "10px", "fontWeight": "bold", "fontSize": "1rem"}
                    ),
                    dcc.Dropdown(
                        id='end-year-dropdown',
                        options=[],
                        value=None,
                        clearable=False,
                        style={"display": "inline-block", "vertical-align": "middle", "width": "120px"}
                    )
                ], width=7)
            ], className="mb-3"),
            
            # Season and month selection
            dbc.Row([
                dbc.Col([
                    html.Label(
                        "Select Season:", 
                        className="form-label mb-2",
                        style={"display": "inline-block", "vertical-align": "middle", "margin-right": "10px", "fontWeight": "bold", "fontSize": "1rem"}
                    ),
                    dbc.ButtonGroup(
                        [
                            dbc.Button("ALL", color="secondary", id="select-all-months", n_clicks=1, size="sm"),
                            dbc.Button("SPRING", color="success", id="select-spring", n_clicks=0, size="sm"),
                            dbc.Button("SUMMER", color="warning", id="select-summer", n_clicks=0, size="sm"),
                            dbc.Button("FALL", color="danger", id="select-fall", n_clicks=0, size="sm"),
                            dbc.Button("WINTER", color="info", id="select-winter", n_clicks=0, size="sm")
                        ],
                        style={"display": "inline-block", "vertical-align": "middle"}
                    )
                ], width=5),

                dbc.Col([
                    html.Label(
                        "Select Months:", 
                        className="form-label mb-2",
                        style={"display": "inline-block", "vertical-align": "middle", "margin-right": "10px", "fontWeight": "bold", "fontSize": "1rem"}
                    ),
                    dcc.Checklist(
                        id='month-checklist',
                        options=[
                            {'label': 'Jan', 'value': 1}, {'label': 'Feb', 'value': 2},
                            {'label': 'Mar', 'value': 3}, {'label': 'Apr', 'value': 4},
                            {'label': 'May', 'value': 5}, {'label': 'Jun', 'value': 6},
                            {'label': 'Jul', 'value': 7}, {'label': 'Aug', 'value': 8},
                            {'label': 'Sep', 'value': 9}, {'label': 'Oct', 'value': 10},
                            {'label': 'Nov', 'value': 11}, {'label': 'Dec', 'value': 12}
                        ],
                        value=list(range(1, 13)),  # Default to all months
                        inline=True,
                        className="checklist-mobile",
                        style={"display": "inline-block", "vertical-align": "middle"}
                    )
                ], width=7)
            ], className="mb-3 mobile-season-stack"),
            
            # Threshold highlighting and download buttons
            dbc.Row([
                dbc.Col([
                    html.Label(
                        "Highlight Threshold Violations:", 
                        className="form-label mb-2",
                        style={"display": "inline-block", "vertical-align": "middle", "margin-right": "10px", "fontWeight": "bold", "fontSize": "1rem"}
                    ),
                    dbc.Switch(
                        id="highlight-thresholds-switch",
                        value=True,
                        style={"display": "inline-block", "vertical-align": "middle"}
                    )
                ], width=6),
                dbc.Col([
                    dbc.Button(
                        [html.I(className="fas fa-download me-2"), "Download Site Data"],
                        id="chemical-download-site-btn",
                        color="success",
                        size="sm",
                        style={'display': 'none', 'marginRight': '10px'}  # Initially hidden
                    ),
                    dbc.Button(
                        [html.I(className="fas fa-download me-2"), "Download All Chemical Data"],
                        id="chemical-download-btn",
                        color="success",
                        size="sm",
                        style={'display': 'none'}  # Initially hidden
                    )
                ], width=6, className="d-flex justify-content-end align-items-center")
            ], className="mb-3"),
            
            # Visualization container
            dbc.Row([
                dbc.Col([
                    html.Div(id='chemical-graph-container')
                ], width=12)
            ], className="mb-2"),
            
            # Description and diagram 
            dbc.Row([
                dbc.Col([
                    html.Div(id='chemical-explanation-container')
                ], width=6, className="d-flex"),
                dbc.Col([
                    html.Div(id='chemical-diagram-container')
                ], width=6, className="d-flex align-items-center")  
            ], className="h-100 align-items-stretch mobile-stack", style={'minHeight': '400px'})
        ], id="chemical-controls-content", style={'display': 'none'}),
    ], className="tab-content-wrapper") 