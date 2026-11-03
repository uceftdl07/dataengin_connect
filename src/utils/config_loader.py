"""
Configuration loader for YAML files (tables, APIs, connector).
Production-ready with proper error handling.
"""
import os
import re
from typing import Any, Dict, List, Optional

import yaml


class ConfigLoader:
    """Load YAML configuration files for a given domain."""

    _IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    _KAFKA_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]+$")

    @staticmethod
    def _require_non_empty_string(value: Any, field_name: str, *, allow_list: bool = False) -> Any:
        if value is None:
            raise ValueError(f"{field_name} is required")
        if not isinstance(value, str):
            raise ValueError(f"{field_name} must be a string")
        cleaned = value.strip()
        if not cleaned:
            raise ValueError(f"{field_name} cannot be empty")
        if not allow_list and any(char.isspace() for char in cleaned):
            raise ValueError(f"{field_name} cannot contain whitespace")
        return cleaned
    
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

    def load_kafka_config(self) -> Dict[str, Any]:
        """Load and validate Kafka topics configured for daily ingestion."""
        config_file = os.path.join(self.domain_path, "kafka_config.yml")

        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Kafka config not found: {config_file}")

        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        if not config:
            raise ValueError(f"Empty configuration file: {config_file}")
        if config.get('domain') != self.domain_name:
            raise ValueError(
                f"Domain mismatch in kafka_config.yml: expected "
                f"{self.domain_name}, got {config.get('domain')}"
            )
        if not isinstance(config.get('topics'), list):
            raise ValueError("No 'topics' list in kafka_config.yml")

        for topic in config['topics']:
            if not isinstance(topic, dict):
                raise ValueError("Each Kafka topic configuration must be a mapping")
            for required in ('topic', 'target_table', 'event_timestamp_field', 'schema'):
                if not topic.get(required):
                    raise ValueError(
                        f"Kafka topic configuration is missing '{required}'"
                    )
            topic_name = self._require_non_empty_string(topic['topic'], "Kafka topic name")
            target_table = self._require_non_empty_string(topic['target_table'], "Kafka target_table")
            timestamp_field = self._require_non_empty_string(
                topic['event_timestamp_field'], "Kafka event_timestamp_field"
            )
            if not self._KAFKA_TOPIC_RE.fullmatch(topic_name):
                raise ValueError(f"Kafka topic '{topic_name}' must be a valid topic name")
            if not self._IDENTIFIER_RE.fullmatch(target_table):
                raise ValueError(f"Kafka target table '{target_table}' must be a valid identifier")
            schema = topic['schema']
            if (
                not isinstance(schema, dict)
                or schema.get('type') != 'struct'
                or not isinstance(schema.get('fields'), list)
            ):
                raise ValueError(
                    f"Kafka topic '{topic_name}' schema must be a Spark struct"
                )
            timestamp_schema = next(
                (
                    field
                    for field in schema['fields']
                    if isinstance(field, dict) and field.get('name') == timestamp_field
                ),
                None,
            )
            if not timestamp_schema or timestamp_schema.get('type') != 'timestamp':
                raise ValueError(
                    f"Kafka topic '{topic_name}' event_timestamp_field "
                    "must name a top-level timestamp schema field"
                )

        return config

    def get_enabled_kafka_topics(self) -> List[Dict[str, Any]]:
        """Return enabled topic configurations."""
        config = self.load_kafka_config()
        return [topic for topic in config['topics'] if topic.get('enabled', True)]
    
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
