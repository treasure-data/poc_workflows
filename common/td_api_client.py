"""
Treasure Data API Client - Common Module

This module provides a unified interface for interacting with Treasure Data APIs
including LLM API and standard TD APIs. It includes proper error handling,
logging, and retry logic for production use.

Author: Treasure Data Engineering Team
"""

import requests
import json
import time
import logging
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logger = logging.getLogger(__name__)

class TreasureDataAPIError(Exception):
    """Custom exception for Treasure Data API errors."""
    pass

class TreasureDataAPIClient:
    """
    A robust client for Treasure Data APIs with built-in retry logic,
    error handling, and logging.
    """
    
    def __init__(self, api_key: str, region: str = "us", timeout: int = 30):
        """
        Initialize the Treasure Data API client.
        
        Args:
            api_key: Treasure Data API key
            region: Region ("us" or "eu")
            timeout: Request timeout in seconds
        """
        self.api_key = api_key
        self.timeout = timeout
        
        # Set up endpoints based on region
        if region.lower() == "eu":
            self.api_endpoint = "https://api.eu01.treasuredata.com"
            self.llm_api_endpoint = "https://llm-api.eu01.treasuredata.com"
        else:
            self.api_endpoint = "https://api.treasuredata.com"
            self.llm_api_endpoint = "https://llm-api.treasuredata.com"
        
        # Configure session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        logger.info(f"Initialized TD API client for region: {region}")

    def _get_headers(self, content_type: str = "application/json") -> Dict[str, str]:
        """Get standard headers for API requests."""
        return {
            'Authorization': f'TD1 {self.api_key}',
            'Content-Type': content_type
        }

    def _get_llm_headers(self) -> Dict[str, str]:
        """Get headers for LLM API requests."""
        return {
            'Authorization': f'TD1 {self.api_key}',
            'Content-Type': 'application/vnd.api+json'
        }

    def _make_request(self, method: str, url: str, headers: Dict[str, str], 
                     data: Optional[Dict] = None, params: Optional[Dict] = None) -> requests.Response:
        """
        Make an HTTP request with proper error handling.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            headers: Request headers
            data: Request body data
            params: URL parameters
            
        Returns:
            Response object
            
        Raises:
            TreasureDataAPIError: For API-specific errors
        """
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP {response.status_code} error"
            if response.status_code == 401:
                error_msg = "Authentication failed. Please check your API key."
            elif response.status_code == 403:
                error_msg = "Permission denied. Please check your API key permissions."
            elif response.status_code == 404:
                error_msg = "Resource not found."
            elif response.status_code == 429:
                error_msg = "Rate limit exceeded. Please try again later."
            
            try:
                error_detail = response.json().get('message', '')
                if error_detail:
                    error_msg += f" Details: {error_detail}"
            except:
                error_msg += f" Response: {response.text[:200]}"
            
            logger.error(error_msg)
            raise TreasureDataAPIError(error_msg) from e
            
        except requests.exceptions.Timeout:
            error_msg = f"Request timeout after {self.timeout} seconds"
            logger.error(error_msg)
            raise TreasureDataAPIError(error_msg)
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error: {str(e)}"
            logger.error(error_msg)
            raise TreasureDataAPIError(error_msg) from e

    def get_database_tables(self, database_name: str) -> List[str]:
        """
        Get a list of all tables in a specified Treasure Data database.
        
        Args:
            database_name: Name of the database
            
        Returns:
            List of table names
            
        Raises:
            TreasureDataAPIError: If database doesn't exist or access denied
        """
        url = f"{self.api_endpoint}/v3/table/list/{database_name}"
        headers = self._get_headers()
        
        logger.info(f"Fetching tables for database: {database_name}")
        
        response = self._make_request("GET", url, headers)
        tables_data = response.json()
        
        if 'tables' not in tables_data:
            raise TreasureDataAPIError(f"Invalid response format for database {database_name}")
        
        table_names = [table['name'] for table in tables_data['tables']]
        logger.info(f"Found {len(table_names)} tables in database {database_name}")
        
        return table_names

    def create_project(self, name: str, description: str = "") -> str:
        """
        Create a new project in Treasure Data.
        
        Args:
            name: Project name
            description: Project description
            
        Returns:
            Project ID
        """
        url = f"{self.llm_api_endpoint}/api/projects"
        headers = self._get_llm_headers()
        
        payload = {
            "data": {
                "type": "projects",
                "attributes": {
                    "name": name,
                    "description": description
                }
            }
        }
        
        logger.info(f"Creating project: {name}")
        
        response = self._make_request("POST", url, headers, payload)
        project_id = response.json()['data']['id']
        
        logger.info(f"Created project with ID: {project_id}")
        return project_id

    def create_knowledge_base(self, name: str, project_id: str, 
                            td_database_name: str, table_names: List[str]) -> str:
        """
        Create a knowledge base with the specified parameters.
        
        Args:
            name: Knowledge base name
            project_id: Parent project ID
            td_database_name: Treasure Data database name
            table_names: List of table names to include
            
        Returns:
            Knowledge base ID
            
        Raises:
            TreasureDataAPIError: If database name is too long or other errors
        """
        if len(td_database_name) > 128:
            raise TreasureDataAPIError("Database name must not exceed 128 characters")
        
        url = f"{self.llm_api_endpoint}/api/knowledge_bases"
        headers = self._get_llm_headers()
        
        tables = [
            {
                "name": table_name,
                "tdQuery": f"SELECT * FROM {table_name}",
                "enableData": False,
                "enableDataIndex": False
            }
            for table_name in table_names
        ]
        
        payload = {
            "data": {
                "type": "knowledge_bases",
                "attributes": {
                    "name": name,
                    "projectId": project_id,
                    "tdDatabaseName": td_database_name,
                    "tables": tables
                }
            }
        }
        
        logger.info(f"Creating knowledge base: {name} with {len(table_names)} tables")
        
        response = self._make_request("POST", url, headers, payload)
        kb_id = response.json()['data']['id']
        
        logger.info(f"Created knowledge base with ID: {kb_id}")
        return kb_id

    def create_agent(self, name: str, project_id: str, system_prompt: str = "",
                    starter_message: str = "", model_type: str = "claude-3-haiku",
                    max_tool_iterations: int = 4, temperature: float = 0,
                    tools: Optional[List[Dict[str, Any]]] = None) -> str:
        """
        Create an agent with the specified parameters.
        
        Args:
            name: Agent name
            project_id: Parent project ID
            system_prompt: System prompt for the agent
            starter_message: Initial message from the agent
            model_type: AI model type to use
            max_tool_iterations: Maximum tool iterations
            temperature: Model temperature
            tools: List of tools for the agent
            
        Returns:
            Agent ID
        """
        url = f"{self.llm_api_endpoint}/api/agents"
        headers = self._get_llm_headers()
        
        payload = {
            "data": {
                "type": "agents",
                "attributes": {
                    "name": name,
                    "systemPrompt": system_prompt,
                    "starterMessage": starter_message,
                    "modelType": model_type,
                    "maxToolIterations": max_tool_iterations,
                    "temperature": temperature,
                    "projectId": project_id,
                    "tools": tools or []
                }
            }
        }
        
        logger.info(f"Creating agent: {name}")
        
        response = self._make_request("POST", url, headers, payload)
        agent_id = response.json()['data']['id']
        
        logger.info(f"Created agent with ID: {agent_id}")
        return agent_id


def create_staging_agent(api_key: str, db_name: str, region: str = "us") -> None:
    """
    Create a staging agent for data cleaning and standardization.
    
    Args:
        api_key: Treasure Data API key
        db_name: Source database name
        region: TD region (us or eu)
    """
    client = TreasureDataAPIClient(api_key, region)
    
    try:
        # Project Configuration
        project_name = "Data Cleaning & Standardization Staging Agent"
        description = "Project for Agents Associated with Supporting Agents that help stage data for unification"
        
        # Knowledge Base Configuration
        kb_name = "src_kb"
        
        # Get database tables
        table_list = client.get_database_tables(db_name)
        
        # Create project
        project_id = client.create_project(project_name, description)
        
        # Create knowledge base
        source_kb_id = client.create_knowledge_base(kb_name, project_id, db_name, table_list)
        
        # Agent Configuration
        agent_name = "Staging Query Agent"
        agent_prompt = """
        You're job is to create a staging query to clean raw customer data tables that are imported into Treasure Data's Customer Data Platform. A user will provide you with a schema in the form of a ddl query or other text or they will ask you to find a specific table from the database. If they provide you a schema do not search the database. For each request provide transformations for every field requested. If they ask you to search a table use the src_kb function to query a sample dataset and use that information to create the query using the parameters below. 

        The objective of the staging query is to clean and standardize data using best practices which will ultimately create staging tables that will be used for unifying data using Treasure Data's unification algorithms (deterministic and probabilistic) and creating derived attributes from the attribute data. Output should be Presto Query.

        Here are examples you can use

        -- General string transformations - MAKE SURE YOU USE THIS METHOD WHEN DEALING WITH STRINGS SO THEY ARE TITLED CASED 

        CASE WHEN nullif(lower(ltrim(rtrim("first_name"))), 'null') is null then null WHEN nullif(lower(ltrim(rtrim("first_name"))), '') is null then null ELSE array_join((transform((split(lower(trim("first_name")),' ')), x -> concat(upper(substr(x,1,1)),substr(x,2,length(x))))),' ','') END AS "trfmd_first_name"

        -- Email transformations 

        CASE WHEN nullif(lower(ltrim(rtrim("email"))), 'null') is null then null WHEN nullif(lower(ltrim(rtrim("email"))), '') is null then null WHEN nullif(lower(trim("id")), '') in (select lower(trim(invalid_email)) from ${stg}${sub}.invalid_emails ) then null ELSE lower(ltrim(rtrim(regexp_replace("email", '[^a-zA-Z0-9.@+-]', '')))) END AS "trfmd_email"

        -- Phone number transformations 
        CASE WHEN nullif(lower(ltrim(rtrim("phone_number"))), 'null') is null then null WHEN nullif(lower(ltrim(rtrim("phone_number"))), '') is null then null ELSE ARRAY_JOIN(REGEXP_EXTRACT_ALL(replace(lower(ltrim(rtrim("phone_number"))), ' ', ''), '([0-9]+)?'), '') END AS "trfmd_phone_number"

        -- ISO8601 Timestamp - Make sure to use TD_TIME_PARSE when transforming ISO8601 Timestamp so the results are in Unixtimestamp

        TD_TIME_PARSE(date_of_birth) as "trfmd_date_of_birth_unix"

        -- Boolean transformations CASE WHEN nullif(lower(ltrim(rtrim("consent_flag"))), 'null') is null then null WHEN nullif(lower(ltrim(rtrim("consent_flag"))), '') is null then null WHEN nullif(lower(ltrim(rtrim("consent_flag"))), '') in ('0', 'false') then 'False' WHEN nullif(lower(ltrim(rtrim("consent_flag"))), '') in ('1', 'true') then 'True' end AS "trfmd_consent_flag"

        When you have access to the columns ignore transforming id fields that are UUID or similar. 
        """
        starter_message = "Which table should I build a staging query for? Please provide table name from connected knowledge or provide schema"
        model_type = 'claude-3.5-sonnet'
        
        tools = [{
            "targetKnowledgeBaseId": source_kb_id,
            "targetFunction": "QUERY_DATA_DIRECT",
            "functionName": "src_kb",
            "functionDescription": "Source DB Knowledge Base"
        }]
        
        # Create agent
        agent_id = client.create_agent(
            agent_name,
            project_id,
            agent_prompt,
            starter_message,
            model_type,
            tools=tools
        )
        
        logger.info(f"Successfully created staging agent with ID: {agent_id}")
        
    except TreasureDataAPIError as e:
        logger.error(f"Failed to create staging agent: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating staging agent: {e}")
        raise TreasureDataAPIError(f"Unexpected error: {e}") from e


def create_knowledge_base_from_database(api_key: str, project_id: str, 
                                      database_name: str, kb_name: str, 
                                      region: str = "us") -> str:
    """
    Create a knowledge base from all tables in a database.
    
    Args:
        api_key: Treasure Data API key
        project_id: Target project ID
        database_name: Source database name
        kb_name: Knowledge base name
        region: TD region (us or eu)
        
    Returns:
        Knowledge base ID
    """
    client = TreasureDataAPIClient(api_key, region)
    
    try:
        # Get the list of tables from the provided database
        table_list = client.get_database_tables(database_name)
        
        # Create the knowledge base using the provided parameters
        kb_id = client.create_knowledge_base(kb_name, project_id, database_name, table_list)
        
        logger.info(f"Successfully created knowledge base '{kb_name}' with ID: {kb_id}")
        return kb_id
        
    except TreasureDataAPIError as e:
        logger.error(f"Failed to create knowledge base: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating knowledge base: {e}")
        raise TreasureDataAPIError(f"Unexpected error: {e}") from e
