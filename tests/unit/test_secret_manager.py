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
