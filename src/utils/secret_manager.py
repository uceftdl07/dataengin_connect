"""
Secret manager for Databricks secret scopes.
Production-ready with proper error handling.
"""
from typing import Dict, Any, Optional


class SecretManager:
    """Manage access to Databricks secrets by domain and source."""
    
    def __init__(self, dbutils):
        """
        Initialize the secret manager.
        
        Args:
            dbutils: Databricks utilities object
        """
        self.dbutils = dbutils
    
    def get_sqlserver_credentials(self, connector_config: Dict[str, Any]) -> Dict[str, str]:
        """
        Retrieve SQL Server credentials from secret scope.
        
        Args:
            connector_config: Connector config with secret_scope and secret_keys
        
        Returns:
            Dict with hostname, port, database, username, password
        
        Raises:
            ValueError: If secrets cannot be retrieved
        """
        scope = connector_config['secret_scope']
        keys = connector_config['secret_keys']
        
        credentials = {}
        for key_name, secret_key in keys.items():
            try:
                credentials[key_name] = self.dbutils.secrets.get(scope=scope, key=secret_key)
            except Exception as e:
                raise ValueError(
                    f"Failed to retrieve secret '{secret_key}' from scope '{scope}': {str(e)}"
                )
        
        return credentials
    
    def get_api_credentials(
        self,
        api_name: str,
        api_secrets_config: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Retrieve credentials for a given API.
        
        Args:
            api_name: API name (e.g., "customer_api")
            api_secrets_config: api_secrets section from connector_config
        
        Returns:
            Dict with API secrets (bearer_token, api_key, client_id/secret, etc.)
        
        Raises:
            ValueError: If API not found or secrets cannot be retrieved
        """
        if api_name not in api_secrets_config:
            raise ValueError(f"API '{api_name}' not found in api_secrets config")
        
        api_secret = api_secrets_config[api_name]
        scope = api_secret['secret_scope']
        keys = api_secret['secret_keys']
        
        credentials = {}
        for key_name, secret_key in keys.items():
            try:
                credentials[key_name] = self.dbutils.secrets.get(scope=scope, key=secret_key)
            except Exception as e:
                raise ValueError(
                    f"Failed to retrieve API secret '{secret_key}' from scope '{scope}': {str(e)}"
                )
        
        return credentials
    
    def build_jdbc_url(
        self,
        credentials: Dict[str, str],
        jdbc_options: Dict[str, Any]
    ) -> str:
        """
        Build JDBC URL for SQL Server.
        
        Args:
            credentials: Dict with hostname, port, database
            jdbc_options: JDBC options (encrypt, trustServerCertificate, etc.)
        
        Returns:
            Complete JDBC URL
        """
        hostname = credentials['hostname']
        port = credentials.get('port', '1433')
        database = credentials['database']
        
        # Base URL
        jdbc_url = f"jdbc:sqlserver://{hostname}:{port};databaseName={database}"
        
        # Add options
        for key, value in jdbc_options.items():
            if key not in ['driver', 'fetchsize', 'batchsize', 'isolationLevel']:
                jdbc_url += f";{key}={value}"
        
        return jdbc_url
    
    def build_jdbc_properties(
        self,
        credentials: Dict[str, str],
        jdbc_options: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Build JDBC properties (user, password, driver, etc.).
        
        Args:
            credentials: Dict with username, password
            jdbc_options: JDBC options
        
        Returns:
            Dict of JDBC properties
        """
        properties = {
            "user": credentials['username'],
            "password": credentials['password'],
            "driver": jdbc_options.get('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')
        }
        
        # Performance options
        if 'fetchsize' in jdbc_options:
            properties['fetchsize'] = str(jdbc_options['fetchsize'])
        
        if 'batchsize' in jdbc_options:
            properties['batchsize'] = str(jdbc_options['batchsize'])
        
        if 'isolationLevel' in jdbc_options:
            properties['isolationLevel'] = jdbc_options['isolationLevel']
        
        return properties
