import requests
import json
from typing import List, Dict, Any

def create_project(td_apikey: str, name: str, description: str) -> str:
    """Create a new project in Treasure Data."""
    create_project_url = "https://llm-api.treasuredata.com/api/projects"
    
    payload = {
        "data": {
            "type": "projects",
            "attributes": {
                "name": name,
                "description": description
            }
        }
    }
    
    headers = {
        'Content-Type': 'application/vnd.api+json',
        'Authorization': f'TD1 {td_apikey}'
    }
    
    response = requests.post(create_project_url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()['data']['id']

def get_database_tables(td_apikey: str, database_name: str) -> List[str]:
    """Get a list of all tables in a specified Treasure Data database."""
    list_tables_url = f"https://api.treasuredata.com/v3/table/list/{database_name}"
    
    headers = {
        'Authorization': f'TD1 {td_apikey}'
    }
    
    try:
        response = requests.get(list_tables_url, headers=headers)
        response.raise_for_status()
        
        tables_data = response.json()
        table_names = [table['name'] for table in tables_data['tables']]
        return table_names
        
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            raise ValueError(f"Database '{database_name}' not found")
        elif response.status_code == 403:
            raise ValueError("Permission denied. Please check your API key and permissions")
        else:
            raise e

def create_knowledge_base(
    td_apikey: str, 
    name: str, 
    project_id: str, 
    td_database_name: str, 
    table_names: List[str]
) -> str:
    """Create a knowledge base with the specified parameters."""
    create_kb_url = "https://llm-api.treasuredata.com/api/knowledge_bases"
    
    if len(td_database_name) > 128:
        raise ValueError("tdDatabaseName must not exceed 128 characters")
    
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
    
    headers = {
        'Content-Type': 'application/vnd.api+json',
        'Authorization': f'TD1 {td_apikey}'
    }
    
    response = requests.post(create_kb_url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()['data']['id']

def create_agent(
    td_apikey: str,
    name: str,
    project_id: str,
    system_prompt: str = "",
    starter_message: str = "",
    model_type: str = "claude-3-haiku",
    max_tool_iterations: int = 4,
    temperature: float = 0,
    tools: List[Dict[str, Any]] = None
) -> str:
    """Create an agent with the specified parameters."""
    create_agent_url = "https://llm-api.treasuredata.com/api/agents"
    
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
    
    headers = {
        'Content-Type': 'application/vnd.api+json',
        'Authorization': f'TD1 {td_apikey}'
    }
    
    response = requests.post(create_agent_url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()['data']['id']

def create_staging_agent(td_apikey: str, db_name: str) -> None:
    """Create a staging agent for data cleaning and standardization."""
    try:
        # Project Configuration
        project_name = "Data Cleaning & Standardization Staging Agent"
        description = "Project for Agents Associated with Supporting Agents that help stage data for unification"
        
        # Knowledge Base Configuration
        kb_name = "src_kb"
        
        # Get database tables
        table_list = get_database_tables(td_apikey, db_name)
        
        # Create project
        project_id = create_project(td_apikey, project_name, description)
        
        # Create knowledge base
        source_kb_id = create_knowledge_base(td_apikey, kb_name, project_id, db_name, table_list)
        
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
        agent_id = create_agent(
            td_apikey,
            agent_name,
            project_id,
            agent_prompt,
            starter_message,
            model_type,
            tools=tools
        )
        
        print(f"Successfully created staging agent with ID: {agent_id}")
        
    except Exception as e:
        print(f"Error creating staging agent: {str(e)}")
        raise
