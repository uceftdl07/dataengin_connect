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
    
    # Supported JDBC authentication strategies (see build_jdbc_properties).
    SUPPORTED_AUTH_TYPES = ("sql", "service_principal", "managed_identity", "access_token")

    def get_sqlserver_credentials(self, connector_config: Dict[str, Any]) -> Dict[str, str]:
        """
        Retrieve connection credentials from the secret scope.

        Only the keys declared in `secret_keys` are fetched, so the same method
        works for every auth strategy:
          - sql               -> hostname, port, database, username, password
          - service_principal -> hostname, port, database, client_id, client_secret
          - managed_identity  -> hostname, port, database (+ optional client_id)
          - access_token      -> hostname, port, database (token generated at runtime)

        Args:
            connector_config: Connector config with secret_scope and secret_keys

        Returns:
            Dict of the resolved secret values keyed by their logical name

        Raises:
            ValueError: If secrets cannot be retrieved
        """
        scope = connector_config['secret_scope']
        keys = connector_config.get('secret_keys', {})

        credentials = {}
        for key_name, secret_key in keys.items():
            try:
                credentials[key_name] = self.dbutils.secrets.get(scope=scope, key=secret_key)
            except Exception as e:
                raise ValueError(
                    f"Failed to retrieve secret '{secret_key}' from scope '{scope}': {str(e)}"
                )
        
        return credentials

    def get_mongodb_connection_uri(self, connector_config: Dict[str, Any]) -> str:
        """Retrieve the MongoDB connection URI from a Databricks secret scope."""
        scope = connector_config.get('secret_scope')
        secret_key = connector_config.get('uri_secret_key')
        if not scope or not secret_key:
            raise ValueError(
                "MongoDB connector requires 'secret_scope' and 'uri_secret_key'"
            )

        try:
            return self.dbutils.secrets.get(scope=scope, key=secret_key)
        except Exception as e:
            raise ValueError(
                f"Failed to retrieve MongoDB connection URI from scope '{scope}' "
                f"using key '{secret_key}': {str(e)}"
            ) from e
    
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
        jdbc_options: Dict[str, Any],
        auth_type: str = "sql"
    ) -> Dict[str, str]:
        """
        Build JDBC properties for the requested authentication strategy.

        auth_type values:
          - "sql"                : classic SQL login (user/password from secrets).
          - "service_principal"  : Microsoft Entra ID service principal.
                                   Uses `authentication=ActiveDirectoryServicePrincipal`
                                   with user=client_id, password=client_secret.
                                   Requires an Entra-enabled DB (Azure SQL / MI / Synapse)
                                   and the msal4j library on the cluster classpath.
          - "managed_identity"   : Entra ID managed identity of the Databricks compute.
                                   Uses `authentication=ActiveDirectoryManagedIdentity`.
                                   NO secret at all. Pass a user-assigned MI client id via
                                   the `client_id` credential (omit for system-assigned).
          - "access_token"       : Pass a pre-generated Entra access token via
                                   `accessToken` (see get_entra_access_token()).

        Args:
            credentials: Resolved credentials (keys depend on auth_type)
            jdbc_options: JDBC options (driver, fetchsize, batchsize, isolationLevel...)
            auth_type: Authentication strategy (default "sql")

        Returns:
            Dict of JDBC properties

        Raises:
            ValueError: If auth_type is unsupported or required credentials are missing
        """
        auth = (auth_type or "sql").lower()
        if auth not in self.SUPPORTED_AUTH_TYPES:
            raise ValueError(
                f"Unsupported auth_type '{auth_type}'. "
                f"Expected one of: {', '.join(self.SUPPORTED_AUTH_TYPES)}"
            )

        properties = {
            "driver": jdbc_options.get('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')
        }

        if auth == "sql":
            properties["user"] = credentials['username']
            properties["password"] = credentials['password']

        elif auth == "service_principal":
            # Entra ID service principal (client credentials).
            properties["authentication"] = "ActiveDirectoryServicePrincipal"
            properties["user"] = credentials['client_id']
            properties["password"] = credentials['client_secret']
            properties.setdefault("encrypt", "true")

        elif auth == "managed_identity":
            # Managed identity of the Databricks compute -> zero secret.
            properties["authentication"] = "ActiveDirectoryManagedIdentity"
            # Only needed for a *user-assigned* managed identity.
            if credentials.get('client_id'):
                properties["msiClientId"] = credentials['client_id']
            properties.setdefault("encrypt", "true")

        elif auth == "access_token":
            token = credentials.get('access_token')
            if not token:
                raise ValueError(
                    "auth_type 'access_token' requires an 'access_token' credential "
                    "(generate one with get_entra_access_token())."
                )
            properties["accessToken"] = token
            properties.setdefault("encrypt", "true")

        # Performance options (apply to every auth strategy)
        if 'fetchsize' in jdbc_options:
            properties['fetchsize'] = str(jdbc_options['fetchsize'])
        
        if 'batchsize' in jdbc_options:
            properties['batchsize'] = str(jdbc_options['batchsize'])
        
        if 'isolationLevel' in jdbc_options:
            properties['isolationLevel'] = jdbc_options['isolationLevel']
        
        return properties

    def get_entra_access_token(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        scope: str = "https://database.windows.net/.default"
    ) -> str:
        """
        Acquire a Microsoft Entra ID access token via a service principal.

        Optional helper for auth_type "access_token". Requires the `msal` package
        on the cluster. For SQL databases keep the default resource scope
        (https://database.windows.net/.default).

        Args:
            client_id: Service principal (application) client id
            client_secret: Service principal client secret
            tenant_id: Entra ID tenant id
            scope: OAuth resource scope for the target database

        Returns:
            A bearer access token string

        Raises:
            RuntimeError: If the token cannot be acquired
        """
        try:
            import msal
        except ImportError as e:
            raise RuntimeError(
                "The 'msal' package is required for access_token auth. "
                "Add it to the job libraries."
            ) from e

        app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
        )
        result = app.acquire_token_for_client(scopes=[scope])
        if "access_token" not in result:
            raise RuntimeError(
                f"Failed to acquire Entra token: "
                f"{result.get('error_description', result.get('error', 'unknown error'))}"
            )
        return result["access_token"]
