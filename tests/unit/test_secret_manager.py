"""
Unit tests for SecretManager.
"""
import pytest
from src.utils.secret_manager import SecretManager


@pytest.mark.unit
class TestSecretManager:
    """Test SecretManager class."""
    
    def test_get_sqlserver_credentials(self, mock_dbutils, sample_connector_config):
        """Test retrieving SQL Server credentials."""
        manager = SecretManager(mock_dbutils)
        credentials = manager.get_sqlserver_credentials(
            sample_connector_config['connector']
        )
        
        assert credentials['hostname'] == 'test-server.database.windows.net'
        assert credentials['port'] == '1433'
        assert credentials['database'] == 'TestDB'
        assert credentials['username'] == 'test_user'
        assert credentials['password'] == 'test_password'
    
    def test_get_sqlserver_credentials_missing_secret(self, mock_dbutils):
        """Test error when secret is missing."""
        connector_config = {
            'secret_scope': 'nonexistent-scope',
            'secret_keys': {
                'hostname': 'hostname'
            }
        }
        
        manager = SecretManager(mock_dbutils)
        
        with pytest.raises(ValueError, match="Failed to retrieve secret"):
            manager.get_sqlserver_credentials(connector_config)
    
    def test_get_api_credentials_bearer(self, mock_dbutils, sample_connector_config):
        """Test retrieving API credentials (bearer token)."""
        manager = SecretManager(mock_dbutils)
        
        # Mock for orders_api with bearer token
        mock_dbutils.secrets.get.side_effect = lambda scope, key: 'test_bearer_token'
        
        api_secrets = {
            'orders_api': {
                'secret_scope': 'api-mv-orders_api',
                'secret_keys': {
                    'bearer_token': 'bearer_token'
                }
            }
        }
        
        credentials = manager.get_api_credentials('orders_api', api_secrets)
        
        assert credentials['bearer_token'] == 'test_bearer_token'
    
    def test_get_api_credentials_oauth2(self, mock_dbutils, sample_connector_config):
        """Test retrieving OAuth2 credentials."""
        manager = SecretManager(mock_dbutils)
        credentials = manager.get_api_credentials(
            'customer_api',
            sample_connector_config['api_secrets']
        )
        
        assert 'oauth_client_id' in credentials
        assert 'oauth_client_secret' in credentials
    
    def test_get_api_credentials_not_found(self, mock_dbutils):
        """Test error when API not in config."""
        manager = SecretManager(mock_dbutils)
        
        with pytest.raises(ValueError, match="API .* not found"):
            manager.get_api_credentials('nonexistent_api', {})
    
    def test_build_jdbc_url(self, mock_dbutils):
        """Test building JDBC URL."""
        manager = SecretManager(mock_dbutils)
        
        credentials = {
            'hostname': 'test-server.database.windows.net',
            'port': '1433',
            'database': 'TestDB'
        }
        
        jdbc_options = {
            'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'encrypt': 'true',
            'trustServerCertificate': 'false'
        }
        
        url = manager.build_jdbc_url(credentials, jdbc_options)
        
        assert url.startswith('jdbc:sqlserver://test-server.database.windows.net:1433')
        assert 'databaseName=TestDB' in url
        assert 'encrypt=true' in url
        assert 'trustServerCertificate=false' in url
        assert 'driver=' not in url  # Driver should not be in URL
    
    def test_build_jdbc_url_default_port(self, mock_dbutils):
        """Test JDBC URL with default port."""
        manager = SecretManager(mock_dbutils)
        
        credentials = {
            'hostname': 'test-server.database.windows.net',
            'database': 'TestDB'
            # No port specified
        }
        
        url = manager.build_jdbc_url(credentials, {})
        
        assert ':1433' in url  # Default port
    
    def test_build_jdbc_properties(self, mock_dbutils):
        """Test building JDBC properties."""
        manager = SecretManager(mock_dbutils)
        
        credentials = {
            'username': 'test_user',
            'password': 'test_password'
        }
        
        jdbc_options = {
            'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'fetchsize': '10000',
            'batchsize': '5000',
            'isolationLevel': 'READ_UNCOMMITTED'
        }
        
        properties = manager.build_jdbc_properties(credentials, jdbc_options)
        
        assert properties['user'] == 'test_user'
        assert properties['password'] == 'test_password'
        assert properties['driver'] == 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        assert properties['fetchsize'] == '10000'
        assert properties['batchsize'] == '5000'
        assert properties['isolationLevel'] == 'READ_UNCOMMITTED'
    
    def test_build_jdbc_properties_defaults(self, mock_dbutils):
        """Test JDBC properties with defaults."""
        manager = SecretManager(mock_dbutils)
        
        credentials = {
            'username': 'test_user',
            'password': 'test_password'
        }
        
        properties = manager.build_jdbc_properties(credentials, {})
        
        assert properties['user'] == 'test_user'
        assert properties['password'] == 'test_password'
        assert properties['driver'] == 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

    def test_build_jdbc_properties_sql_explicit(self, mock_dbutils):
        """Explicit auth_type='sql' behaves like the default."""
        manager = SecretManager(mock_dbutils)

        credentials = {'username': 'test_user', 'password': 'test_password'}

        properties = manager.build_jdbc_properties(
            credentials, {}, auth_type='sql'
        )

        assert properties['user'] == 'test_user'
        assert properties['password'] == 'test_password'
        assert 'authentication' not in properties

    def test_build_jdbc_properties_service_principal(self, mock_dbutils):
        """Service principal auth maps client_id/secret to Entra auth."""
        manager = SecretManager(mock_dbutils)

        credentials = {
            'client_id': 'app-client-id',
            'client_secret': 'app-client-secret'
        }

        properties = manager.build_jdbc_properties(
            credentials, {}, auth_type='service_principal'
        )

        assert properties['authentication'] == 'ActiveDirectoryServicePrincipal'
        assert properties['user'] == 'app-client-id'
        assert properties['password'] == 'app-client-secret'
        assert properties['encrypt'] == 'true'

    def test_build_jdbc_properties_managed_identity_system(self, mock_dbutils):
        """System-assigned managed identity requires no secret and no client id."""
        manager = SecretManager(mock_dbutils)

        properties = manager.build_jdbc_properties(
            {}, {}, auth_type='managed_identity'
        )

        assert properties['authentication'] == 'ActiveDirectoryManagedIdentity'
        assert 'user' not in properties
        assert 'password' not in properties
        assert 'msiClientId' not in properties

    def test_build_jdbc_properties_managed_identity_user_assigned(self, mock_dbutils):
        """User-assigned managed identity passes the msiClientId."""
        manager = SecretManager(mock_dbutils)

        properties = manager.build_jdbc_properties(
            {'client_id': 'mi-client-id'}, {}, auth_type='managed_identity'
        )

        assert properties['authentication'] == 'ActiveDirectoryManagedIdentity'
        assert properties['msiClientId'] == 'mi-client-id'
        assert 'password' not in properties

    def test_build_jdbc_properties_access_token(self, mock_dbutils):
        """Access token auth passes the token via the accessToken property."""
        manager = SecretManager(mock_dbutils)

        properties = manager.build_jdbc_properties(
            {'access_token': 'entra-token-value'}, {}, auth_type='access_token'
        )

        assert properties['accessToken'] == 'entra-token-value'
        assert 'user' not in properties
        assert 'password' not in properties

    def test_build_jdbc_properties_access_token_missing(self, mock_dbutils):
        """Access token auth without a token raises a clear error."""
        manager = SecretManager(mock_dbutils)

        with pytest.raises(ValueError, match="access_token"):
            manager.build_jdbc_properties(
                {}, {}, auth_type='access_token'
            )

    def test_build_jdbc_properties_invalid_auth_type(self, mock_dbutils):
        """An unknown auth_type is rejected."""
        manager = SecretManager(mock_dbutils)

        with pytest.raises(ValueError, match="Unsupported auth_type"):
            manager.build_jdbc_properties(
                {'username': 'u', 'password': 'p'}, {}, auth_type='kerberos'
            )

    def test_build_jdbc_properties_perf_options_all_auth(self, mock_dbutils):
        """Performance options are applied regardless of auth strategy."""
        manager = SecretManager(mock_dbutils)

        jdbc_options = {
            'fetchsize': '10000',
            'batchsize': '5000',
            'isolationLevel': 'READ_UNCOMMITTED'
        }

        properties = manager.build_jdbc_properties(
            {'client_id': 'id', 'client_secret': 'secret'},
            jdbc_options,
            auth_type='service_principal'
        )

        assert properties['fetchsize'] == '10000'
        assert properties['batchsize'] == '5000'
        assert properties['isolationLevel'] == 'READ_UNCOMMITTED'

    def test_get_entra_access_token(self, mock_dbutils, monkeypatch):
        """get_entra_access_token returns the token from MSAL."""
        import sys
        import types

        fake_msal = types.ModuleType('msal')

        class _FakeApp:
            def __init__(self, *args, **kwargs):
                pass

            def acquire_token_for_client(self, scopes):
                return {'access_token': 'fake-entra-token'}

        fake_msal.ConfidentialClientApplication = _FakeApp
        monkeypatch.setitem(sys.modules, 'msal', fake_msal)

        manager = SecretManager(mock_dbutils)
        token = manager.get_entra_access_token(
            client_id='id', client_secret='secret', tenant_id='tenant'
        )

        assert token == 'fake-entra-token'

    def test_get_entra_access_token_failure(self, mock_dbutils, monkeypatch):
        """A failed token acquisition raises RuntimeError."""
        import sys
        import types

        fake_msal = types.ModuleType('msal')

        class _FakeApp:
            def __init__(self, *args, **kwargs):
                pass

            def acquire_token_for_client(self, scopes):
                return {'error': 'invalid_client', 'error_description': 'bad secret'}

        fake_msal.ConfidentialClientApplication = _FakeApp
        monkeypatch.setitem(sys.modules, 'msal', fake_msal)

        manager = SecretManager(mock_dbutils)
        with pytest.raises(RuntimeError, match="Failed to acquire Entra token"):
            manager.get_entra_access_token(
                client_id='id', client_secret='secret', tenant_id='tenant'
            )
