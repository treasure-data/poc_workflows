"""
Unit tests for the Treasure Data API client.

These tests ensure the API client handles errors correctly,
validates inputs properly, and maintains consistent behavior.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import requests
import sys
import os

# Add common module to path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'common'))

from td_api_client import TreasureDataAPIClient, TreasureDataAPIError


class TestTreasureDataAPIClient(unittest.TestCase):
    """Test cases for TreasureDataAPIClient."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.api_key = "test_api_key"
        self.client = TreasureDataAPIClient(self.api_key, region="us")
    
    def test_initialization_us_region(self):
        """Test client initialization with US region."""
        client = TreasureDataAPIClient("test_key", region="us")
        self.assertEqual(client.api_endpoint, "https://api.treasuredata.com")
        self.assertEqual(client.llm_api_endpoint, "https://llm-api.treasuredata.com")
    
    def test_initialization_eu_region(self):
        """Test client initialization with EU region."""
        client = TreasureDataAPIClient("test_key", region="eu")
        self.assertEqual(client.api_endpoint, "https://api.eu01.treasuredata.com")
        self.assertEqual(client.llm_api_endpoint, "https://llm-api.eu01.treasuredata.com")
    
    def test_get_headers(self):
        """Test header generation."""
        headers = self.client._get_headers()
        expected = {
            'Authorization': 'TD1 test_api_key',
            'Content-Type': 'application/json'
        }
        self.assertEqual(headers, expected)
    
    def test_get_llm_headers(self):
        """Test LLM API header generation."""
        headers = self.client._get_llm_headers()
        expected = {
            'Authorization': 'TD1 test_api_key',
            'Content-Type': 'application/vnd.api+json'
        }
        self.assertEqual(headers, expected)
    
    @patch('td_api_client.requests.Session.request')
    def test_make_request_success(self, mock_request):
        """Test successful API request."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        response = self.client._make_request(
            "GET", 
            "https://api.example.com/test",
            {"Authorization": "Bearer test"}
        )
        
        self.assertEqual(response, mock_response)
        mock_request.assert_called_once()
    
    @patch('td_api_client.requests.Session.request')
    def test_make_request_http_error(self, mock_request):
        """Test HTTP error handling."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_response.json.return_value = {"message": "Invalid API key"}
        mock_request.return_value = mock_response
        
        with self.assertRaises(TreasureDataAPIError) as context:
            self.client._make_request(
                "GET",
                "https://api.example.com/test", 
                {"Authorization": "Bearer test"}
            )
        
        self.assertIn("Authentication failed", str(context.exception))
    
    @patch('td_api_client.requests.Session.request')
    def test_make_request_timeout(self, mock_request):
        """Test timeout handling."""
        mock_request.side_effect = requests.exceptions.Timeout()
        
        with self.assertRaises(TreasureDataAPIError) as context:
            self.client._make_request(
                "GET",
                "https://api.example.com/test",
                {"Authorization": "Bearer test"}
            )
        
        self.assertIn("Request timeout", str(context.exception))
    
    @patch('td_api_client.TreasureDataAPIClient._make_request')
    def test_get_database_tables_success(self, mock_request):
        """Test successful database table listing."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "tables": [
                {"name": "table1"},
                {"name": "table2"},
                {"name": "table3"}
            ]
        }
        mock_request.return_value = mock_response
        
        tables = self.client.get_database_tables("test_db")
        
        self.assertEqual(tables, ["table1", "table2", "table3"])
        mock_request.assert_called_once()
    
    @patch('td_api_client.TreasureDataAPIClient._make_request')
    def test_get_database_tables_invalid_response(self, mock_request):
        """Test invalid response handling for database tables."""
        mock_response = Mock()
        mock_response.json.return_value = {"invalid": "response"}
        mock_request.return_value = mock_response
        
        with self.assertRaises(TreasureDataAPIError) as context:
            self.client.get_database_tables("test_db")
        
        self.assertIn("Invalid response format", str(context.exception))
    
    @patch('td_api_client.TreasureDataAPIClient._make_request')
    def test_create_project_success(self, mock_request):
        """Test successful project creation."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": {"id": "project_123"}
        }
        mock_request.return_value = mock_response
        
        project_id = self.client.create_project("Test Project", "Test Description")
        
        self.assertEqual(project_id, "project_123")
        mock_request.assert_called_once()
    
    def test_create_knowledge_base_invalid_db_name(self):
        """Test knowledge base creation with invalid database name."""
        long_name = "a" * 129  # Exceeds 128 character limit
        
        with self.assertRaises(TreasureDataAPIError) as context:
            self.client.create_knowledge_base(
                "test_kb", 
                "project_123", 
                long_name, 
                ["table1"]
            )
        
        self.assertIn("must not exceed 128 characters", str(context.exception))


class TestAPIClientIntegration(unittest.TestCase):
    """Integration tests for API client functionality."""
    
    @patch('td_api_client.TreasureDataAPIClient.get_database_tables')
    @patch('td_api_client.TreasureDataAPIClient.create_project')
    @patch('td_api_client.TreasureDataAPIClient.create_knowledge_base') 
    @patch('td_api_client.TreasureDataAPIClient.create_agent')
    def test_create_staging_agent_workflow(self, mock_create_agent, 
                                         mock_create_kb, mock_create_project,
                                         mock_get_tables):
        """Test the complete staging agent creation workflow."""
        # Mock the workflow steps
        mock_get_tables.return_value = ["table1", "table2"]
        mock_create_project.return_value = "project_123"
        mock_create_kb.return_value = "kb_456"
        mock_create_agent.return_value = "agent_789"
        
        from td_api_client import create_staging_agent
        
        # This should not raise any exceptions
        create_staging_agent("test_api_key", "test_db", "us")
        
        # Verify all steps were called
        mock_get_tables.assert_called_once_with("test_db")
        mock_create_project.assert_called_once()
        mock_create_kb.assert_called_once()
        mock_create_agent.assert_called_once()


if __name__ == '__main__':
    unittest.main()
