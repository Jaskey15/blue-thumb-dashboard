"""
Chemical callbacks for the Tenmile Creek Water Quality Dashboard.
"""

import dash
from dash import Input, Output, State, dcc, html

from data_processing.chemical_utils import KEY_PARAMETERS, get_reference_values
from data_processing.data_queries import (
    get_chemical_data_from_db,
    get_chemical_date_range,
)
from utils import get_sites_with_data, setup_logging

from .helper_functions import create_empty_state, create_error_state
from .tab_utilities import (
    create_all_parameters_visualization,
    create_single_parameter_visualization,
)

# Configure logging
logger = setup_logging("chemical_callbacks", category="callbacks")

def register_chemical_callbacks(app):
    """Register all chemical-related callbacks in logical workflow order."""
    
    # STATE MANAGEMENT
    @app.callback(
        Output('chemical-tab-state', 'data'),
        [Input('chemical-site-dropdown', 'value'),
         Input('chemical-parameter-dropdown', 'value'),
         Input('start-year-dropdown', 'value'),
         Input('end-year-dropdown', 'value'),
         Input('month-checklist', 'value'),
         Input('highlight-thresholds-switch', 'value')],
        [State('chemical-tab-state', 'data')],
        prevent_initial_call=True
    )
    def save_chemical_state(selected_site, selected_parameter, start_year, end_year, selected_months, 
                           highlight_thresholds, current_state):
        """Save chemical tab state when selections change."""
        # Convert start_year and end_year to year_range for consistency
        year_range = None
        if start_year is not None and end_year is not None:
            year_range = [start_year, end_year]
        
        # Only save valid selections, don't overwrite with None
        if any(val is not None for val in [selected_site, selected_parameter, start_year, end_year, selected_months, highlight_thresholds]):
            # Preserve existing values when only some change
            new_state = current_state.copy() if current_state else {
                'selected_site': None,
                'selected_parameter': None,
                'year_range': None,
                'selected_months': None,
                'highlight_thresholds': None
            }
            
            if selected_site is not None:
                new_state['selected_site'] = selected_site
                logger.info(f"Saving chemical site state: {selected_site}")
            
            if selected_parameter is not None:
                new_state['selected_parameter'] = selected_parameter
                logger.info(f"Saving chemical parameter state: {selected_parameter}")
            
            if year_range is not None:
                new_state['year_range'] = year_range
                logger.info(f"Saving chemical year range state: {year_range}")
            
            if selected_months is not None:
                new_state['selected_months'] = selected_months
                logger.info(f"Saving chemical months state: {len(selected_months) if selected_months else 0} months")
            
            if highlight_thresholds is not None:
                new_state['highlight_thresholds'] = highlight_thresholds
                logger.info(f"Saving chemical highlight thresholds state: {highlight_thresholds}")
            
            return new_state
        else:
            # Keep existing state when all controls are cleared
            return current_state or {
                'selected_site': None,
                'selected_parameter': None,
                'year_range': None,
                'selected_months': None,
                'highlight_thresholds': None
            }
    
    # NAVIGATION AND DROPDOWN POPULATION
    @app.callback(
        [Output('chemical-site-dropdown', 'options'),
         Output('chemical-site-dropdown', 'value', allow_duplicate=True),
         Output('chemical-parameter-dropdown', 'value', allow_duplicate=True),
         Output('start-year-dropdown', 'options', allow_duplicate=True),
         Output('start-year-dropdown', 'value', allow_duplicate=True),
         Output('end-year-dropdown', 'options', allow_duplicate=True),
         Output('end-year-dropdown', 'value', allow_duplicate=True),
         Output('month-checklist', 'value', allow_duplicate=True),
         Output('highlight-thresholds-switch', 'value', allow_duplicate=True)],
        [Input('main-tabs', 'active_tab'),
         Input('navigation-store', 'data')],
        [State('chemical-tab-state', 'data')],
        prevent_initial_call=True
    )
    def handle_chemical_navigation_and_state_restoration(active_tab, nav_data, chemical_state):
        """Handle navigation from map, initial tab loading, and state restoration for all chemical controls."""
        if active_tab != 'chemical-tab':
            return [], dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
        
        # Get the trigger to understand what caused this callback
        ctx = dash.callback_context
        if not ctx.triggered:
            return [], dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

        try:
            # Get all sites with chemical data
            sites = get_sites_with_data('chemical')

            if not sites:
                logger.warning("No sites with chemical data found")
                return [], dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

            # Create dropdown options
            options = [{'label': site, 'value': site} for site in sorted(sites)]
            logger.info(f"Populated chemical dropdown with {len(options)} sites")

            # Get year range for populating year dropdowns
            min_year, max_year = get_chemical_date_range()
            year_options = [{'label': str(year), 'value': year} for year in range(min_year, max_year + 1)]

            # Priority 1: Handle navigation from map (highest priority)
            if (nav_data and nav_data.get('target_tab') == 'chemical-tab' and
                nav_data.get('target_site')):

                target_site = nav_data.get('target_site')
                target_parameter = nav_data.get('target_parameter')

                if target_site in sites:
                    # For map navigation, set site + parameter from nav, preserve other filters from state or use defaults
                    restored_year_range = (chemical_state.get('year_range')
                                         if chemical_state and chemical_state.get('year_range')
                                         else [min_year, max_year])
                    restored_months = (chemical_state.get('selected_months')
                                     if chemical_state and chemical_state.get('selected_months')
                                     else dash.no_update)
                    restored_thresholds = (chemical_state.get('highlight_thresholds')
                                         if chemical_state and chemical_state.get('highlight_thresholds') is not None
                                         else dash.no_update)

                    return (
                        options,  # Site options
                        target_site,  # Set target site
                        target_parameter or 'do_percent',  # Set target parameter
                        year_options,  # Start year options
                        restored_year_range[0],  # Start year value
                        year_options,  # End year options
                        restored_year_range[1],  # End year value
                        restored_months,  # Preserve months from state
                        restored_thresholds  # Preserve thresholds from state
                    )
                else:
                    logger.warning(f"Navigation target site '{target_site}' not found in available sites")

            # Priority 2: Restore from saved state if tab was just activated AND no active navigation
            if (trigger_id == 'main-tabs' and chemical_state and
                chemical_state.get('selected_site') and
                (not nav_data or not nav_data.get('target_tab'))):

                saved_site = chemical_state.get('selected_site')
                saved_parameter = chemical_state.get('selected_parameter')
                saved_year_range = chemical_state.get('year_range')
                saved_months = chemical_state.get('selected_months')
                saved_thresholds = chemical_state.get('highlight_thresholds')

                # Verify saved site is still available
                if saved_site in sites:
                    # Convert year_range to individual start/end years
                    start_year = saved_year_range[0] if saved_year_range else min_year
                    end_year = saved_year_range[1] if saved_year_range else max_year

                    return (
                        options,  # Site options
                        saved_site,  # Restore site
                        saved_parameter,  # Restore parameter
                        year_options,  # Start year options
                        start_year,  # Restore start year
                        year_options,  # End year options
                        end_year,  # Restore end year
                        saved_months,  # Restore months
                        saved_thresholds  # Restore thresholds
                    )
                else:
                    logger.warning(f"Saved site '{saved_site}' no longer available")

            # Ignore navigation-store clearing events (when nav_data is empty/None)
            if trigger_id == 'navigation-store' and (not nav_data or not nav_data.get('target_tab')):
                return options, dash.no_update, dash.no_update, year_options, dash.no_update, year_options, dash.no_update, dash.no_update, dash.no_update

            # Default behavior - populate site and year options with defaults
            return options, dash.no_update, dash.no_update, year_options, min_year, year_options, max_year, dash.no_update, dash.no_update

        except Exception as e:
            logger.error(f"Error in chemical navigation/state restoration: {e}")
            return [], dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    
    # SITE SELECTION & CONTROLS

    @app.callback(
        [Output('chemical-controls-content', 'style'),
         Output('chemical-download-btn', 'style'),
         Output('chemical-download-site-btn', 'style')],
        [Input('chemical-site-dropdown', 'value'),
         Input('chemical-parameter-dropdown', 'value')]
    )
    def show_chemical_controls(selected_site, selected_parameter):
        """Show parameter controls and download buttons when a site and parameter are selected."""
        if selected_site and selected_parameter:
            logger.info(f"Chemical site selected: {selected_site}, parameter: {selected_parameter}")
            return {'display': 'block'}, {'display': 'block'}, {'display': 'block', 'marginRight': '10px'}
        elif selected_site:
            logger.info(f"Chemical site selected: {selected_site}")
            return {'display': 'block'}, {'display': 'none'}, {'display': 'none'}
        return {'display': 'none'}, {'display': 'none'}, {'display': 'none'}
    
    # DATA VISUALIZATION & FILTERS
    @app.callback(
        Output('month-checklist', 'value'),
        [Input('select-all-months', 'n_clicks'),
         Input('select-spring', 'n_clicks'),
         Input('select-summer', 'n_clicks'), 
         Input('select-fall', 'n_clicks'),
         Input('select-winter', 'n_clicks')],
        prevent_initial_call=True
    )
    def update_month_selection(all_clicks, spring_clicks, summer_clicks, fall_clicks, winter_clicks):
        """Update month selection based on season button clicks."""
        ctx = dash.callback_context
        
        if not ctx.triggered:
            return dash.no_update
        
        # Get which button was clicked
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        # Define season mappings
        season_months = {
            'select-all-months': list(range(1, 13)),  # All months
            'select-spring': [3, 4, 5],      # March, April, May
            'select-summer': [6, 7, 8],      # June, July, August  
            'select-fall': [9, 10, 11],      # September, October, November
            'select-winter': [12, 1, 2],     # December, January, February
        }
        
        return season_months.get(button_id, dash.no_update)
    
    @app.callback(
        [Output('start-year-dropdown', 'options'),
         Output('end-year-dropdown', 'options')],
        [Input('start-year-dropdown', 'value'),
         Input('end-year-dropdown', 'value')]
    )
    def update_year_dropdown_options(start_year, end_year):
        """Update year dropdown options to ensure end year >= start year."""
        min_year, max_year = get_chemical_date_range()
        
        # Create all year options
        all_years = list(range(min_year, max_year + 1))
        
        # For start year dropdown: all years
        start_options = [{'label': str(year), 'value': year} for year in all_years]
        
        # For end year dropdown: only years >= start_year (if start_year is selected)
        if start_year is not None:
            valid_end_years = [year for year in all_years if year >= start_year]
        else:
            valid_end_years = all_years
            
        end_options = [{'label': str(year), 'value': year} for year in valid_end_years]
        
        return start_options, end_options
    
    @app.callback(
        [Output('chemical-graph-container', 'children'),
         Output('chemical-explanation-container', 'children'),
         Output('chemical-diagram-container', 'children')],
        [Input('chemical-parameter-dropdown', 'value'),
         Input('start-year-dropdown', 'value'),
         Input('end-year-dropdown', 'value'),
         Input('month-checklist', 'value'),
         Input('highlight-thresholds-switch', 'value'),
         Input('chemical-site-dropdown', 'value')]
    )
    def update_chemical_display(selected_parameter, start_year, end_year, selected_months, 
                              highlight_thresholds, selected_site):
        """Update chemical parameter visualization based on user selections."""
        
        # Validate inputs
        if not selected_site:
            return create_empty_state("Please select a site to view data."), html.Div(), html.Div()
        
        if not selected_parameter:
            return create_empty_state("Please select a parameter to visualize."), html.Div(), html.Div()
        
        # Validate year range - if None, get default range
        if start_year is None or end_year is None:
            min_year, max_year = get_chemical_date_range()
            start_year = start_year if start_year is not None else min_year
            end_year = end_year if end_year is not None else max_year
            logger.info(f"Using default year range: {start_year} to {end_year}")
        
        # Create year_range for filtering
        year_range = [start_year, end_year]
        
        # Validate selected_months - if None, use all months
        if selected_months is None:
            selected_months = list(range(1, 13))
            logger.info(f"Using default months: all months")
        
        # Validate highlight_thresholds - if None, use default True
        if highlight_thresholds is None:
            highlight_thresholds = True
            logger.info(f"Using default highlight thresholds: {highlight_thresholds}")
        
        try:
            logger.info(f"Creating chemical visualization for {selected_site}, parameter: {selected_parameter}")
            
            # Get processed data
            df_filtered = get_chemical_data_from_db(selected_site)
            key_parameters = KEY_PARAMETERS
            reference_values = get_reference_values()
            
            # Filter by year range and months
            if not df_filtered.empty:
                df_filtered = df_filtered[
                    (df_filtered['Year'] >= year_range[0]) & 
                    (df_filtered['Year'] <= year_range[1])
                ]
                
                if selected_months:
                    df_filtered = df_filtered[df_filtered['Month'].isin(selected_months)]
            
            # Create visualization based on parameter selection
            if selected_parameter == 'all_parameters':
                graph, explanation, diagram = create_all_parameters_visualization(
                    df_filtered, key_parameters, reference_values, highlight_thresholds, selected_site
                )
            else:
                graph, explanation, diagram = create_single_parameter_visualization(
                    df_filtered, selected_parameter, reference_values, highlight_thresholds, selected_site
                )
                
            return graph, explanation, diagram
                
        except Exception as e:
            logger.error(f"Error creating chemical visualization: {e}")
            error_state = create_error_state(
                "Error Loading Chemical Data",
                f"Could not load chemical data for {selected_site}. Please try again.",
                str(e)
            )
            return error_state, html.Div(), html.Div()

    # DATA DOWNLOAD
    @app.callback(
        Output('chemical-download-component', 'data'),
        [Input('chemical-download-btn', 'n_clicks'),
         Input('chemical-download-site-btn', 'n_clicks')],
        [State('chemical-site-dropdown', 'value')],
        prevent_initial_call=True
    )
    def download_chemical_data(all_clicks, site_clicks, selected_site):
        """Download chemical data CSV file - all data or site-specific from database."""
        if not all_clicks and not site_clicks:
            return dash.no_update
        
        ctx = dash.callback_context
        if not ctx.triggered:
            return dash.no_update
        
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        try:
            # Handle site-specific download
            if button_id == 'chemical-download-site-btn':
                if not selected_site:
                    logger.warning("No site selected for site-specific download")
                    return dash.no_update
                
                site_df = get_chemical_data_from_db(selected_site)
                
                if site_df.empty:
                    logger.warning(f"No chemical data found for site: {selected_site}")
                    return dash.no_update
                
                core_data_columns = [col for col in site_df.columns if not col.endswith('_status')]
                site_df_export = site_df[core_data_columns].copy()
                
                site_name = selected_site.replace(' ', '_').replace(':', '').replace('(', '').replace(')', '').replace(',', '')
                filename = f"{site_name}_chemical_data.csv"
                
                logger.info(f"Successfully prepared chemical data export for {selected_site} with {len(site_df_export)} records")
                
                return dcc.send_data_frame(
                    site_df_export.to_csv,
                    filename,
                    index=False
                )
            
            # Handle all data download
            elif button_id == 'chemical-download-btn':
                logger.info("Downloading all chemical data from database")
                
                chemical_df = get_chemical_data_from_db()
                
                if chemical_df.empty:
                    logger.warning("No chemical data found in database")
                    return dash.no_update
                
                core_data_columns = [col for col in chemical_df.columns if not col.endswith('_status')]
                chemical_df_export = chemical_df[core_data_columns].copy()
                
                filename = f"blue_thumb_chemical_data.csv"
                
                logger.info(f"Successfully prepared chemical data export with {len(chemical_df_export)} records")
                
                return dcc.send_data_frame(
                    chemical_df_export.to_csv,
                    filename,
                    index=False
                )
                
        except Exception as e:
            logger.error(f"Error downloading chemical data: {e}")
            return dash.no_update


