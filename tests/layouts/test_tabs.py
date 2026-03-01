"""
Tests for layouts.tabs module

This file tests the tab layout creation functions including:
- Tab layout generation
- Component hierarchy validation
- Tab-specific functionality
- Content organization
"""

import os
import sys
import unittest
from unittest.mock import patch

import dash_bootstrap_components as dbc
from dash import dcc, html

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from layouts.tabs import (
    create_biological_tab,
    create_chemical_tab,
    create_habitat_tab,
    create_overview_tab,
    create_protect_our_streams_tab,
    create_source_data_tab,
)


def get_tab_content(tab_wrapper):
    """
    Helper function to extract the main tab content from a tab wrapper.
    Handles two structures:
    1. Wrapper div containing tab content + chatbot separately (biological, source_data, etc.)
    2. Tab content div that includes chatbot as a child (chemical)
    
    Args:
        tab_wrapper: The wrapper div returned by tab creation functions
        
    Returns:
        The main tab content div (with tab-content-wrapper class)
    """
    # First check if this component itself has the tab-content-wrapper class
    if (hasattr(tab_wrapper, 'className') and 
        tab_wrapper.className and 
        'tab-content-wrapper' in tab_wrapper.className):
        return tab_wrapper
    
    # Otherwise, look for child with tab-content-wrapper class
    if hasattr(tab_wrapper, 'children') and tab_wrapper.children:
        for child in tab_wrapper.children:
            if (hasattr(child, 'className') and 
                child.className and 
                'tab-content-wrapper' in child.className):
                return child
    return None


def safe_get_children(component):
    """
    Safely get children from a component, handling components without children.
    
    Args:
        component: Dash component
        
    Returns:
        List of children or empty list if no children
    """
    if hasattr(component, 'children') and component.children is not None:
        if isinstance(component.children, list):
            return component.children
        else:
            return [component.children]
    return []


def find_component_by_id(root, component_id, component_type=None):
    """
    Recursively find a component by ID in the component tree.
    
    Args:
        root: Root component to search from
        component_id: ID to search for
        component_type: Optional component type filter
        
    Returns:
        Found component or None
    """
    # Check if this component matches
    if hasattr(root, 'id') and root.id == component_id:
        if component_type is None or isinstance(root, component_type):
            return root
    
    # Search children
    for child in safe_get_children(root):
        result = find_component_by_id(child, component_id, component_type)
        if result is not None:
            return result
    
    return None


def find_components_by_type(root, component_type):
    """
    Recursively find all components of a specific type.
    
    Args:
        root: Root component to search from
        component_type: Type of component to find
        
    Returns:
        List of found components
    """
    components = []
    
    # Check if this component matches
    if isinstance(root, component_type):
        components.append(root)
    
    # Search children
    for child in safe_get_children(root):
        components.extend(find_components_by_type(child, component_type))
    
    return components


class TestOverviewTab(unittest.TestCase):
    """Test overview tab layout creation."""
    
    @patch('layouts.tabs.overview.load_markdown_content')
    def test_create_overview_tab(self, mock_markdown):
        """Test overview tab creation."""
        # Mock markdown loading
        mock_markdown.return_value = html.Div("Mock markdown content")
        
        tab_wrapper = create_overview_tab()
        tab_content = get_tab_content(tab_wrapper)
        
        # Check it's a Div component
        self.assertIsInstance(tab_wrapper, html.Div)
        self.assertIsNotNone(tab_content)
        
        # Check has correct class
        self.assertIn("tab-content-wrapper", tab_content.className)
        
        # Check structure (should have multiple rows)
        children = safe_get_children(tab_content)
        self.assertGreater(len(children), 3)  # At least 4 rows expected
        
        # All children should be Row components
        for child in children:
            self.assertIsInstance(child, dbc.Row)
    
    @patch('layouts.tabs.overview.load_markdown_content')
    def test_overview_tab_parameter_dropdown(self, mock_markdown):
        """Test overview tab parameter dropdown component."""
        mock_markdown.return_value = html.Div("Mock content")
        
        tab_wrapper = create_overview_tab()
        
        # Find the parameter dropdown in the structure
        dropdown = find_component_by_id(tab_wrapper, 'parameter-dropdown', dcc.Dropdown)
        
        self.assertIsNotNone(dropdown)
        self.assertEqual(dropdown.id, 'parameter-dropdown')
        self.assertTrue(dropdown.disabled)  # Should start disabled
        self.assertIsNone(dropdown.value)  # Should start with no value
    
    @patch('layouts.tabs.overview.load_markdown_content')
    def test_overview_tab_map_container(self, mock_markdown):
        """Test overview tab map container."""
        mock_markdown.return_value = html.Div("Mock content")
        
        tab_wrapper = create_overview_tab()
        
        # Find the map graph
        map_graph = find_component_by_id(tab_wrapper, 'site-map-graph', dcc.Graph)
        
        self.assertIsNotNone(map_graph)
        self.assertEqual(map_graph.id, 'site-map-graph')
        
        # Check map configuration
        config = map_graph.config
        self.assertTrue(config['scrollZoom'])
        self.assertTrue(config['displayModeBar'])


class TestChemicalTab(unittest.TestCase):
    """Test chemical tab layout creation."""
    
    def test_create_chemical_tab(self):
        """Test chemical tab creation."""
        tab_wrapper = create_chemical_tab()
        tab_content = get_tab_content(tab_wrapper)
        
        # Check it's a Div component
        self.assertIsInstance(tab_wrapper, html.Div)
        self.assertIsNotNone(tab_content)
        
        # Check has correct class
        self.assertIn("tab-content-wrapper", tab_content.className)
        
        # Check structure
        children = safe_get_children(tab_content)
        self.assertGreater(len(children), 2)  # Download, description, site selection, controls
    
    def test_chemical_tab_site_dropdown(self):
        """Test chemical tab site dropdown component."""
        tab_wrapper = create_chemical_tab()
        
        # Find the site dropdown
        site_dropdown = find_component_by_id(tab_wrapper, 'chemical-site-dropdown', dcc.Dropdown)
        
        self.assertIsNotNone(site_dropdown)
        self.assertEqual(site_dropdown.id, 'chemical-site-dropdown')
        self.assertTrue(site_dropdown.searchable)
        self.assertTrue(site_dropdown.clearable)
        self.assertEqual(site_dropdown.placeholder, "Search for a site...")
    
    def test_chemical_tab_parameter_dropdown(self):
        """Test chemical tab parameter dropdown component."""
        tab_wrapper = create_chemical_tab()
        
        # Find the parameter dropdown
        param_dropdown = find_component_by_id(tab_wrapper, 'chemical-parameter-dropdown', dcc.Dropdown)
        
        self.assertIsNotNone(param_dropdown)
        
        # Check parameter options
        expected_values = ['do_percent', 'pH', 'soluble_nitrogen', 'Phosphorus', 'Chloride', 'all_parameters']
        actual_values = [opt['value'] for opt in param_dropdown.options]
        self.assertEqual(actual_values, expected_values)


class TestBiologicalTab(unittest.TestCase):
    """Test biological tab layout creation."""
    
    def test_create_biological_tab(self):
        """Test biological tab creation."""
        tab_wrapper = create_biological_tab()
        tab_content = get_tab_content(tab_wrapper)
        
        # Check it's a Div component
        self.assertIsInstance(tab_wrapper, html.Div)
        self.assertIsNotNone(tab_content)
        
        # Check has correct class
        self.assertIn("tab-content-wrapper", tab_content.className)
        
        # Check basic structure
        children = safe_get_children(tab_content)
        self.assertGreater(len(children), 3)  # Download, description, community selection, site selection, content
    
    def test_biological_tab_community_selector(self):
        """Test biological tab community selector."""
        tab_wrapper = create_biological_tab()
        
        # Find community dropdown
        community_dropdown = find_component_by_id(tab_wrapper, 'biological-community-dropdown', dcc.Dropdown)
        
        self.assertIsNotNone(community_dropdown)
        self.assertEqual(community_dropdown.id, 'biological-community-dropdown')
        
        # Check community options
        expected_values = ['fish', 'macro']
        actual_values = [opt['value'] for opt in community_dropdown.options]
        self.assertEqual(actual_values, expected_values)
        
        # Check default state
        self.assertEqual(community_dropdown.value, '')
    
    def test_biological_tab_site_selector(self):
        """Test biological tab site selector."""
        tab_wrapper = create_biological_tab()
        
        # Find site dropdown
        site_dropdown = find_component_by_id(tab_wrapper, 'biological-site-dropdown', dcc.Dropdown)
        
        self.assertIsNotNone(site_dropdown)
        self.assertEqual(site_dropdown.id, 'biological-site-dropdown')
        self.assertTrue(site_dropdown.searchable)
        self.assertTrue(site_dropdown.clearable)
        self.assertTrue(site_dropdown.disabled)  # Should start disabled


class TestHabitatTab(unittest.TestCase):
    """Test habitat tab layout creation."""
    
    @patch('layouts.tabs.habitat.load_markdown_content')
    @patch('layouts.tabs.habitat.create_image_with_caption')
    def test_create_habitat_tab(self, mock_image, mock_markdown):
        """Test habitat tab creation."""
        # Mock dependencies
        mock_markdown.return_value = html.Div("Mock markdown")
        mock_image.return_value = html.Img()
        
        tab_wrapper = create_habitat_tab()
        tab_content = get_tab_content(tab_wrapper)
        
        # Check it's a Div component
        self.assertIsInstance(tab_wrapper, html.Div)
        self.assertIsNotNone(tab_content)
        
        # Check has correct class
        self.assertIn("tab-content-wrapper", tab_content.className)
        
        # Check structure
        children = safe_get_children(tab_content)
        self.assertGreater(len(children), 2)  # Download, description, site selection, content
    
    @patch('layouts.tabs.habitat.load_markdown_content')
    @patch('layouts.tabs.habitat.create_image_with_caption')
    def test_habitat_tab_site_dropdown(self, mock_image, mock_markdown):
        """Test habitat tab site dropdown."""
        mock_markdown.return_value = html.Div("Mock markdown")
        mock_image.return_value = html.Img()
        
        tab_wrapper = create_habitat_tab()
        
        # Find site dropdown
        site_dropdown = find_component_by_id(tab_wrapper, 'habitat-site-dropdown', dcc.Dropdown)
        
        self.assertIsNotNone(site_dropdown)
        self.assertEqual(site_dropdown.id, 'habitat-site-dropdown')
        self.assertTrue(site_dropdown.searchable)
        self.assertTrue(site_dropdown.clearable)
        self.assertEqual(site_dropdown.placeholder, "Search for a site...")
    
    @patch('layouts.tabs.habitat.load_markdown_content')
    @patch('layouts.tabs.habitat.create_image_with_caption')
    def test_habitat_tab_content_container(self, mock_image, mock_markdown):
        """Test habitat tab content container."""
        mock_markdown.return_value = html.Div("Mock markdown")
        mock_image.return_value = html.Img()
        
        tab_wrapper = create_habitat_tab()
        
        # Find controls content container
        controls_div = find_component_by_id(tab_wrapper, 'habitat-controls-content', html.Div)
        
        self.assertIsNotNone(controls_div)
        
        # Should be initially hidden
        self.assertEqual(controls_div.style['display'], 'none')


class TestProtectStreamsTab(unittest.TestCase):
    """Test protect streams tab layout creation."""
    
    @patch('layouts.tabs.protect_streams.load_markdown_content')
    @patch('layouts.tabs.protect_streams.create_image_with_caption')
    def test_create_protect_streams_tab(self, mock_image, mock_markdown):
        """Test protect streams tab creation."""
        # Mock dependencies
        mock_markdown.return_value = html.Div("Mock markdown content")
        mock_image.return_value = html.Img()
        
        tab_wrapper = create_protect_our_streams_tab()
        tab_content = get_tab_content(tab_wrapper)
        
        # Check it's a Div component
        self.assertIsInstance(tab_wrapper, html.Div)
        self.assertIsNotNone(tab_content)
        
        # Check has correct class
        self.assertIn("tab-content-wrapper", tab_content.className)
        
        # Check structure - should have rows
        children = safe_get_children(tab_content)
        self.assertGreater(len(children), 1)  # At least intro row and actions row
        
        # Check all children are rows
        for child in children:
            self.assertIsInstance(child, dbc.Row)
    
    @patch('layouts.tabs.protect_streams.load_markdown_content')
    @patch('layouts.tabs.protect_streams.create_image_with_caption')
    def test_protect_streams_tab_tabs_structure(self, mock_image, mock_markdown):
        """Test protect streams tab has proper tabs structure."""
        mock_markdown.return_value = html.Div("Mock markdown content")
        mock_image.return_value = html.Img()
        
        tab_wrapper = create_protect_our_streams_tab()
        
        # Find the tabs component
        tabs_components = find_components_by_type(tab_wrapper, dbc.Tabs)
        
        # Should find at least one tabs component
        self.assertGreater(len(tabs_components), 0)
        
        tabs_component = tabs_components[0]
        
        # Should have 4 tabs (Home & Yard, Rural & Agricultural, Recreation, Community Action)
        self.assertEqual(len(tabs_component.children), 4)
        
        # Check tab labels
        expected_labels = ["Home & Yard", "Rural & Agricultural", "Recreation", "Community Action"]
        actual_labels = [tab_child.label for tab_child in tabs_component.children]
        self.assertEqual(actual_labels, expected_labels)


class TestSourceDataTab(unittest.TestCase):
    """Test source data tab layout creation."""
    
    def test_create_source_data_tab(self):
        """Test source data tab creation."""
        tab_wrapper = create_source_data_tab()
        tab_content = get_tab_content(tab_wrapper)
        
        # Check it's a Div component
        self.assertIsInstance(tab_wrapper, html.Div)
        self.assertIsNotNone(tab_content)
        
        # Check has correct class
        self.assertIn("tab-content-wrapper", tab_content.className)
        
        # Check structure
        children = safe_get_children(tab_content)
        self.assertGreater(len(children), 1)  # Title row and cards row
        
        # Check all children are rows
        for child in children:
            self.assertIsInstance(child, dbc.Row)
    
    def test_source_data_tab_cards(self):
        """Test source data tab has proper card structure."""
        tab_wrapper = create_source_data_tab()
        tab_content = get_tab_content(tab_wrapper)
        
        # Find all cards in the structure
        cards = find_components_by_type(tab_content, dbc.Card)
        
        # Should have multiple cards
        self.assertGreater(len(cards), 1)
        
        # Each card should have header and body
        for card in cards:
            self.assertEqual(len(card.children), 2)
            self.assertIsInstance(card.children[0], dbc.CardHeader)
            self.assertIsInstance(card.children[1], dbc.CardBody)


class TestTabIntegration(unittest.TestCase):
    """Test tab integration and consistency."""
    
    @patch('layouts.tabs.overview.load_markdown_content')
    @patch('layouts.tabs.habitat.load_markdown_content')
    @patch('layouts.tabs.habitat.create_image_with_caption')
    @patch('layouts.tabs.protect_streams.load_markdown_content')
    @patch('layouts.tabs.protect_streams.create_image_with_caption')
    def test_all_tabs_return_valid_components(self, mock_protect_image, mock_protect_markdown,
                                               mock_habitat_image, mock_habitat_markdown,
                                               mock_overview_markdown):
        """Test that all tab creation functions return valid Dash components."""
        # Mock all dependencies
        mock_overview_markdown.return_value = html.Div("Mock content")
        mock_habitat_markdown.return_value = html.Div("Mock content")
        mock_protect_markdown.return_value = html.Div("Mock content")
        mock_habitat_image.return_value = html.Img()
        mock_protect_image.return_value = html.Img()
        
        # Test all tab creation functions
        overview_tab = create_overview_tab()
        chemical_tab = create_chemical_tab()
        biological_tab = create_biological_tab()
        habitat_tab = create_habitat_tab()
        protect_tab = create_protect_our_streams_tab()
        source_tab = create_source_data_tab()
        
        # All should return Div components
        tabs = [overview_tab, chemical_tab, biological_tab, habitat_tab, protect_tab, source_tab]
        for tab in tabs:
            self.assertIsInstance(tab, html.Div)
    
    def test_tab_styling_consistency(self):
        """Test consistent styling across tabs."""
        # Test tabs that don't require mocking
        biological_tab_wrapper = create_biological_tab()
        source_tab_wrapper = create_source_data_tab()
        
        # Extract tab content
        biological_tab_content = get_tab_content(biological_tab_wrapper)
        source_tab_content = get_tab_content(source_tab_wrapper)
        
        # Both should have tab-content-wrapper class
        self.assertIsNotNone(biological_tab_content)
        self.assertIsNotNone(source_tab_content)
        self.assertIn("tab-content-wrapper", biological_tab_content.className)
        self.assertIn("tab-content-wrapper", source_tab_content.className)
    
    def test_tab_download_components_consistency(self):
        """Test that tabs consistently include download components."""
        chemical_tab_wrapper = create_chemical_tab()
        biological_tab_wrapper = create_biological_tab()
        
        # Find download components
        chemical_download = find_component_by_id(chemical_tab_wrapper, 'chemical-download-component', dcc.Download)
        biological_download = find_component_by_id(biological_tab_wrapper, 'biological-download-component', dcc.Download)
        
        # Both should have download components
        self.assertIsNotNone(chemical_download)
        self.assertIsNotNone(biological_download)


if __name__ == '__main__':
    unittest.main(verbosity=2) 