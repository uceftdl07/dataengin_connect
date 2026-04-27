"""
Configuration loader for YAML files (tables, APIs, connector).
Production-ready with proper error handling.
"""
import yaml
import os
from typing import Dict, Any, List, Optional


class ConfigLoader:
    """Load YAML configuration files for a given domain."""
    
    def __init__(self, project_root: str, domain_name: str):
        """
        Initialize the config loader.
        
        Args:
            project_root: Root path of the project
            domain_name: Domain name (mv, wolf, etc.)
        
        Raises:
            ValueError: If domain path doesn't exist
        """
        self.project_root = project_root
        self.domain_name = domain_name
        self.domain_path = os.path.join(project_root, "config", "domains", domain_name)
        
        if not os.path.exists(self.domain_path):
            raise ValueError(f"Domain path not found: {self.domain_path}")
    
    def load_tables_config(self) -> Dict[str, Any]:
        """
        Load SQL Server tables configuration.
        
        Returns:
            Dict containing complete config (domain, tables, global_options)
        
        Raises:
            FileNotFoundError: If tables_config.yml doesn't exist
            ValueError: If config is invalid
        """
        config_file = os.path.join(self.domain_path, "tables_config.yml")
        
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Tables config not found: {config_file}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Validation
        if not config:
            raise ValueError(f"Empty configuration file: {config_file}")
        
        if 'domain' not in config or config['domain'] != self.domain_name:
            raise ValueError(
                f"Domain mismatch in tables_config.yml: "
                f"expected {self.domain_name}, got {config.get('domain')}"
            )
        
        if 'tables' not in config:
            raise ValueError("No 'tables' section in tables_config.yml")
        
        return config
    
    def load_apis_config(self) -> Dict[str, Any]:
        """
        Load REST APIs configuration.
        
        Returns:
            Dict containing complete config (domain, apis, global_options)
        
        Raises:
            FileNotFoundError: If apis_config.yml doesn't exist
            ValueError: If config is invalid
        """
        config_file = os.path.join(self.domain_path, "apis_config.yml")
        
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"APIs config not found: {config_file}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if not config:
            raise ValueError(f"Empty configuration file: {config_file}")
        
        if 'domain' not in config or config['domain'] != self.domain_name:
            raise ValueError(f"Domain mismatch in apis_config.yml")
        
        if 'apis' not in config:
            raise ValueError("No 'apis' section in apis_config.yml")
        
        return config
    
    def load_connector_config(self) -> Dict[str, Any]:
        """
        Load connector configuration (SQL Server + API secrets).
        
        Returns:
            Dict containing connector and api_secrets
        
        Raises:
            FileNotFoundError: If connector_config.yml doesn't exist
            ValueError: If config is invalid
        """
        config_file = os.path.join(self.domain_path, "connector_config.yml")
        
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Connector config not found: {config_file}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if not config:
            raise ValueError(f"Empty configuration file: {config_file}")
        
        if 'domain' not in config or config['domain'] != self.domain_name:
            raise ValueError(f"Domain mismatch in connector_config.yml")
        
        if 'connector' not in config:
            raise ValueError("No 'connector' section in connector_config.yml")
        
        return config
    
    def get_enabled_tables(self) -> List[Dict[str, Any]]:
        """
        Return only enabled tables (enabled=True).
        
        Returns:
            List of dict representing active tables
        """
        config = self.load_tables_config()
        tables = config.get('tables', [])
        return [t for t in tables if t.get('enabled', True)]
    
    def get_enabled_apis(self) -> List[Dict[str, Any]]:
        """
        Return only enabled APIs (enabled=True).
        
        Returns:
            List of dict representing active APIs
        """
        config = self.load_apis_config()
        apis = config.get('apis', [])
        return [a for a in apis if a.get('enabled', True)]
    
    def get_global_table_options(self) -> Dict[str, Any]:
        """
        Get global options for tables.
        
        Returns:
            Dict with global options or empty dict
        """
        config = self.load_tables_config()
        return config.get('global_options', {})
    
    def get_global_api_options(self) -> Dict[str, Any]:
        """
        Get global options for APIs.
        
        Returns:
            Dict with global options or empty dict
        """
        config = self.load_apis_config()
        return config.get('global_options', {})
