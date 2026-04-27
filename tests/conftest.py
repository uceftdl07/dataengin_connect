"""
Pytest fixtures for testing the ingestion framework.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import tempfile
import os


# ============================================================================
# Spark Session Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("pytest-ingestion-framework") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


# ============================================================================
# Mock dbutils
# ============================================================================

@pytest.fixture
def mock_dbutils():
    """Mock Databricks dbutils."""
    dbutils = MagicMock()
    
    # Mock secrets
    secrets_data = {
        'sqlserver-mv': {
            'hostname': 'test-server.database.windows.net',
            'port': '1433',
            'database': 'TestDB',
            'username': 'test_user',
            'password': 'test_password'
        },
        'api-mv-customer_api': {
            'oauth_client_id': 'test_client_id',
            'oauth_client_secret': 'test_client_secret'
        },
        'api-mv-orders_api': {
            'bearer_token': 'test_bearer_token'
        },
        'api-mv-inventory_api': {
            'api_key': 'test_api_key'
        }
    }
    
    def get_secret(scope, key):
        if scope in secrets_data and key in secrets_data[scope]:
            return secrets_data[scope][key]
        raise Exception(f"Secret not found: {scope}/{key}")
    
    dbutils.secrets.get.side_effect = get_secret
    
    return dbutils


# ============================================================================
# Configuration Fixtures
# ============================================================================

@pytest.fixture
def sample_table_config():
    """Sample table configuration."""
    return {
        'table_name': 'dbo.TestTable',
        'target_table': 'test_table',
        'load_mode': 'full',
        'enabled': True,
        'write_mode': 'unity_catalog',
        'adls_path': 'test/full',
        'partition_by': None,
        'read_options': {
            'numPartitions': 2,
            'fetchsize': 1000
        }
    }


@pytest.fixture
def sample_incremental_table_config():
    """Sample incremental table configuration."""
    return {
        'table_name': 'dbo.Orders',
        'target_table': 'orders',
        'load_mode': 'incremental',
        'enabled': True,
        'incremental_column': 'OrderDate',
        'incremental_type': 'timestamp',
        'merge_key': 'OrderID',
        'write_mode': 'unity_catalog',
        'adls_path': 'orders/incremental',
        'partition_by': ['order_year', 'order_month'],
        'derived_columns': {
            'order_year': 'year(OrderDate)',
            'order_month': 'month(OrderDate)'
        },
        'read_options': {
            'numPartitions': 4,
            'fetchsize': 5000
        }
    }


@pytest.fixture
def sample_api_config():
    """Sample API configuration."""
    return {
        'api_name': 'test_api',
        'target_table': 'api_test',
        'enabled': True,
        'base_url': 'https://api.example.com',
        'endpoint': '/v1/data',
        'http_method': 'GET',
        'auth_type': 'bearer',
        'pagination': {
            'type': 'offset',
            'page_size': 100,
            'page_size_param': 'limit',
            'offset_param': 'offset',
            'max_pages': 10
        },
        'headers': {
            'Accept': 'application/json'
        },
        'rate_limit': {
            'requests_per_second': 10,
            'burst': 20
        },
        'retry_config': {
            'max_retries': 3,
            'retry_delay_seconds': 1,
            'backoff_factor': 2,
            'retry_on_status': [429, 500, 503]
        },
        'timeout_seconds': 30,
        'response_path': 'data',
        'load_mode': 'full',
        'write_mode': 'unity_catalog'
    }


@pytest.fixture
def sample_connector_config():
    """Sample connector configuration."""
    return {
        'domain': 'mv',
        'connector': {
            'type': 'sqlserver',
            'secret_scope': 'sqlserver-mv',
            'secret_keys': {
                'hostname': 'hostname',
                'port': 'port',
                'database': 'database',
                'username': 'username',
                'password': 'password'
            },
            'jdbc_options': {
                'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                'encrypt': 'true',
                'trustServerCertificate': 'false',
                'fetchsize': '10000'
            }
        },
        'api_secrets': {
            'customer_api': {
                'secret_scope': 'api-mv-customer_api',
                'secret_keys': {
                    'oauth_client_id': 'oauth_client_id',
                    'oauth_client_secret': 'oauth_client_secret'
                }
            }
        }
    }


@pytest.fixture
def sample_global_options():
    """Sample global options."""
    return {
        'add_metadata_columns': True,
        'metadata_columns': [
            {
                'name': '_ingestion_timestamp',
                'type': 'timestamp',
                'expression': 'current_timestamp()'
            },
            {
                'name': '_source_domain',
                'type': 'string',
                'expression': "'mv'"
            }
        ],
        'delta_options': {
            'optimizeWrite': True,
            'autoCompact': True
        },
        'error_handling': {
            'max_retries': 3,
            'retry_delay_seconds': 60,
            'on_error': 'fail'
        }
    }


# ============================================================================
# Test Data Fixtures
# ============================================================================

@pytest.fixture
def sample_dataframe(spark):
    """Create a sample DataFrame for testing."""
    data = [
        (1, "Alice", "2024-01-01 10:00:00"),
        (2, "Bob", "2024-01-02 11:00:00"),
        (3, "Charlie", "2024-01-03 12:00:00")
    ]
    
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("created_at", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_api_response_data():
    """Sample API response data."""
    return {
        'data': [
            {'id': 1, 'name': 'Item 1', 'value': 100},
            {'id': 2, 'name': 'Item 2', 'value': 200},
            {'id': 3, 'name': 'Item 3', 'value': 300}
        ],
        'pagination': {
            'total': 3,
            'page': 1,
            'per_page': 100
        }
    }


# ============================================================================
# File System Fixtures
# ============================================================================

@pytest.fixture
def temp_config_dir(tmp_path):
    """Create temporary config directory structure."""
    config_dir = tmp_path / "config" / "domains" / "test_domain"
    config_dir.mkdir(parents=True)
    return config_dir


@pytest.fixture
def temp_yaml_files(temp_config_dir):
    """Create temporary YAML config files."""
    import yaml
    
    # tables_config.yml
    tables_config = {
        'domain': 'test_domain',
        'description': 'Test domain',
        'tables': [
            {
                'table_name': 'dbo.TestTable',
                'target_table': 'test_table',
                'load_mode': 'full',
                'enabled': True,
                'write_mode': 'unity_catalog',
                'read_options': {'numPartitions': 2}
            }
        ],
        'global_options': {
            'add_metadata_columns': True
        }
    }
    
    with open(temp_config_dir / 'tables_config.yml', 'w') as f:
        yaml.dump(tables_config, f)
    
    # apis_config.yml
    apis_config = {
        'domain': 'test_domain',
        'apis': [
            {
                'api_name': 'test_api',
                'target_table': 'api_test',
                'enabled': True,
                'base_url': 'https://api.example.com',
                'endpoint': '/data',
                'auth_type': 'bearer'
            }
        ],
        'global_options': {}
    }
    
    with open(temp_config_dir / 'apis_config.yml', 'w') as f:
        yaml.dump(apis_config, f)
    
    # connector_config.yml
    connector_config = {
        'domain': 'test_domain',
        'connector': {
            'type': 'sqlserver',
            'secret_scope': 'test-scope',
            'secret_keys': {
                'hostname': 'hostname'
            }
        },
        'api_secrets': {}
    }
    
    with open(temp_config_dir / 'connector_config.yml', 'w') as f:
        yaml.dump(connector_config, f)
    
    return temp_config_dir


# ============================================================================
# Mock Connector Fixtures
# ============================================================================

@pytest.fixture
def mock_sql_connector():
    """Mock SQL Server connector."""
    connector = Mock()
    connector.test_connection.return_value = True
    connector.read_table.return_value = None  # Will be set per test
    connector.read_incremental.return_value = None
    connector.get_max_value.return_value = None
    return connector


@pytest.fixture
def mock_api_connector():
    """Mock API connector."""
    connector = Mock()
    connector.fetch_all_data.return_value = [
        {'id': 1, 'name': 'Item 1'},
        {'id': 2, 'name': 'Item 2'}
    ]
    return connector
