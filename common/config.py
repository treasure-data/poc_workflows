"""
Configuration management for Treasure Data POC workflows.

This module provides centralized configuration management with validation,
environment variable support, and default values.
"""

import os
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class TDConfig:
    """Configuration class for Treasure Data connections."""
    
    api_key: str
    region: str = "us"
    timeout: int = 30
    max_retries: int = 3
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.api_key:
            raise ValueError("API key is required")
        
        if self.region not in ["us", "eu"]:
            raise ValueError("Region must be 'us' or 'eu'")
        
        if self.timeout <= 0:
            raise ValueError("Timeout must be positive")
        
        if self.max_retries < 0:
            raise ValueError("Max retries cannot be negative")

    @property
    def api_endpoint(self) -> str:
        """Get the API endpoint for the configured region."""
        if self.region == "eu":
            return "https://api.eu01.treasuredata.com"
        return "https://api.treasuredata.com"
    
    @property 
    def llm_api_endpoint(self) -> str:
        """Get the LLM API endpoint for the configured region."""
        if self.region == "eu":
            return "https://llm-api.eu01.treasuredata.com"
        return "https://llm-api.treasuredata.com"

@dataclass
class ExportConfig:
    """Configuration class for data export operations."""
    
    database: str
    table: str
    columns: str
    api_limit: int = 1000
    lower_limit: int = 0
    upper_limit: int = 10000
    api_endpoint: str = ""
    api_key: str = ""
    
    def __post_init__(self):
        """Validate export configuration."""
        if not self.database:
            raise ValueError("Database name is required")
        
        if not self.table:
            raise ValueError("Table name is required")
        
        if not self.columns:
            raise ValueError("Columns specification is required")
        
        if self.api_limit <= 0:
            raise ValueError("API limit must be positive")
        
        if self.lower_limit < 0:
            raise ValueError("Lower limit cannot be negative")
        
        if self.upper_limit <= self.lower_limit:
            raise ValueError("Upper limit must be greater than lower limit")


def load_td_config_from_env() -> TDConfig:
    """
    Load Treasure Data configuration from environment variables.
    
    Returns:
        TDConfig instance
        
    Raises:
        ValueError: If required environment variables are missing
    """
    api_key = os.getenv("TD_API_KEY")
    if not api_key:
        raise ValueError("TD_API_KEY environment variable is required")
    
    region = os.getenv("TD_REGION", "us").lower()
    timeout = int(os.getenv("TD_TIMEOUT", "30"))
    max_retries = int(os.getenv("TD_MAX_RETRIES", "3"))
    
    return TDConfig(
        api_key=api_key,
        region=region, 
        timeout=timeout,
        max_retries=max_retries
    )


def load_export_config_from_env() -> ExportConfig:
    """
    Load export configuration from environment variables.
    
    Returns:
        ExportConfig instance
        
    Raises:
        ValueError: If required environment variables are missing
    """
    database = os.getenv("TD_DATABASE")
    if not database:
        raise ValueError("TD_DATABASE environment variable is required")
    
    table = os.getenv("TD_TABLE") 
    if not table:
        raise ValueError("TD_TABLE environment variable is required")
    
    columns = os.getenv("TD_COLUMNS")
    if not columns:
        raise ValueError("TD_COLUMNS environment variable is required")
    
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("API_KEY environment variable is required")
    
    try:
        api_limit = int(os.getenv("API_LIMIT", "1000"))
        lower_limit = int(os.getenv("LOWER_LIMIT", "0")) 
        upper_limit = int(os.getenv("UPPER_LIMIT", "10000"))
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid numeric environment variable: {e}")
    
    api_endpoint = os.getenv("API_ENDPOINT", "")
    
    return ExportConfig(
        database=database,
        table=table,
        columns=columns,
        api_limit=api_limit,
        lower_limit=lower_limit,
        upper_limit=upper_limit,
        api_endpoint=api_endpoint,
        api_key=api_key
    )


def validate_environment_variables(required_vars: Dict[str, str]) -> Dict[str, Any]:
    """
    Validate that required environment variables are set.
    
    Args:
        required_vars: Dict mapping var names to descriptions
        
    Returns:
        Dict of validated environment variables
        
    Raises:
        ValueError: If any required variables are missing
    """
    missing_vars = []
    config = {}
    
    for var_name, description in required_vars.items():
        value = os.getenv(var_name)
        if not value:
            missing_vars.append(f"{var_name} ({description})")
        else:
            config[var_name] = value
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return config


def setup_logging(level: str = "INFO") -> None:
    """
    Set up standardized logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('workflow.log', mode='a')
        ]
    )
    
    # Suppress verbose logs from requests/urllib3
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    logger.info(f"Logging configured at {level} level")
