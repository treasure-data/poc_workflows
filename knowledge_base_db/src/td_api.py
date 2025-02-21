import requests
from typing import List, Dict, Any

def get_database_tables(td_apikey: str, database_name: str) -> List[str]:
    """Get a list of all tables in a specified Treasure Data database."""
    list_tables_url = f"https://api.treasuredata.com/v3/table/list/{database_name}"
    headers = {
        'Authorization': f'TD1 {td_apikey}'
    }
    response = requests.get(list_tables_url, headers=headers)
    response.raise_for_status()
    tables_data = response.json()
    table_names = [table['name'] for table in tables_data['tables']]
    return table_names

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
