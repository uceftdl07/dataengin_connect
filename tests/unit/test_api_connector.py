"""
Unit tests for GenericApiConnector.
"""
import copy
import pytest
import responses
import time
from src.connectors.generic_api_connector import GenericApiConnector


@pytest.mark.unit
@pytest.mark.api
class TestGenericApiConnector:
    """Test GenericApiConnector class."""
    
    def test_init_bearer_auth(self, sample_api_config):
        """Test initialization with bearer token auth."""
        credentials = {'bearer_token': 'test_token'}
        connector = GenericApiConnector(sample_api_config, credentials)
        
        assert connector.auth_type == 'bearer'
        assert 'Authorization' in connector.session.headers
        assert connector.session.headers['Authorization'] == 'Bearer test_token'
    
    def test_init_api_key_auth(self, sample_api_config):
        """Test initialization with API key auth."""
        config = copy.deepcopy(sample_api_config)
        config['auth_type'] = 'api_key'
        config['auth_config'] = {'api_key_header': 'X-API-Key'}
        
        credentials = {'api_key': 'test_key'}
        connector = GenericApiConnector(config, credentials)
        
        assert 'X-API-Key' in connector.session.headers
        assert connector.session.headers['X-API-Key'] == 'test_key'
    
    def test_init_basic_auth(self, sample_api_config):
        """Test initialization with basic auth."""
        config = copy.deepcopy(sample_api_config)
        config['auth_type'] = 'basic'
        
        credentials = {'username': 'user', 'password': 'pass'}
        connector = GenericApiConnector(config, credentials)
        
        assert connector.session.auth == ('user', 'pass')
    
    def test_init_no_auth(self, sample_api_config):
        """Test initialization without auth."""
        config = copy.deepcopy(sample_api_config)
        config['auth_type'] = 'none'
        
        connector = GenericApiConnector(config, {})
        
        assert connector.auth_type == 'none'
    
    def test_init_oauth2_auth(self, sample_api_config):
        """Test initialization with OAuth2."""
        config = copy.deepcopy(sample_api_config)
        config['auth_type'] = 'oauth2_client_credentials'
        config['auth_config'] = {
            'token_url': 'https://auth.example.com/token',
            'scope': 'read:data'
        }
        
        credentials = {
            'client_id': 'client_id',
            'client_secret': 'client_secret'
        }
        
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                'https://auth.example.com/token',
                json={'access_token': 'oauth_token'},
                status=200
            )
            
            connector = GenericApiConnector(config, credentials)
            
            assert 'Authorization' in connector.session.headers
            assert connector.session.headers['Authorization'] == 'Bearer oauth_token'
    
    def test_init_missing_credentials(self, sample_api_config):
        """Test error when credentials are missing."""
        with pytest.raises(ValueError, match="Bearer token not found"):
            GenericApiConnector(sample_api_config, {})
    
    @responses.activate
    def test_fetch_single_page(self, sample_api_config, sample_api_response_data):
        """Test fetching a single page."""
        responses.add(
            responses.GET,
            'https://api.example.com/v1/data',
            json=sample_api_response_data,
            status=200
        )
        
        credentials = {'bearer_token': 'test_token'}
        connector = GenericApiConnector(sample_api_config, credentials)
        
        data = connector._fetch_single_page()
        
        assert len(data) == 3
        assert data[0]['id'] == 1
    
    @responses.activate
    def test_pagination_offset(self, sample_api_config):
        """Test offset pagination."""
        # Page 1
        responses.add(
            responses.GET,
            'https://api.example.com/v1/data',
            json={'data': [{'id': 1}, {'id': 2}]},
            status=200
        )
        
        # Page 2
        responses.add(
            responses.GET,
            'https://api.example.com/v1/data',
            json={'data': [{'id': 3}]},
            status=200
        )
        
        # Page 3 (empty — stops iteration)
        responses.add(
            responses.GET,
            'https://api.example.com/v1/data',
            json={'data': []},
            status=200
        )
        
        # Use page_size=2: page 1 fills exactly, page 2 has 1 item (<2) → stops.
        config = copy.deepcopy(sample_api_config)
        config['pagination']['page_size'] = 2
        
        credentials = {'bearer_token': 'test_token'}
        connector = GenericApiConnector(config, credentials)
        
        all_data = connector.fetch_all_data()
        
        assert len(all_data) == 3
        assert all_data[0]['id'] == 1
        assert all_data[2]['id'] == 3
    
    @responses.activate
    def test_retry_on_failure(self, sample_api_config):
        """Test retry logic on failure."""
        # Fail twice, then succeed
        responses.add(
            responses.GET,
            'https://api.example.com/v1/data',
            status=500
        )
        responses.add(
            responses.GET,
            'https://api.example.com/v1/data',
            status=503
        )
        responses.add(
            responses.GET,
            'https://api.example.com/v1/data',
            json={'data': [{'id': 1}]},
            status=200
        )
        
        config = copy.deepcopy(sample_api_config)
        config['retry_config']['retry_delay_seconds'] = 0.1  # Speed up test
        
        credentials = {'bearer_token': 'test_token'}
        connector = GenericApiConnector(config, credentials)
        
        data = connector._fetch_single_page()
        
        assert len(data) == 1
        assert len(responses.calls) == 3  # 2 failures + 1 success
    
    @responses.activate
    def test_max_retries_exceeded(self, sample_api_config):
        """Test failure when max retries exceeded."""
        # Always fail
        for _ in range(5):
            responses.add(
                responses.GET,
                'https://api.example.com/v1/data',
                status=500
            )
        
        config = copy.deepcopy(sample_api_config)
        config['retry_config']['max_retries'] = 2
        config['retry_config']['retry_delay_seconds'] = 0.1
        
        credentials = {'bearer_token': 'test_token'}
        connector = GenericApiConnector(config, credentials)
        
        with pytest.raises(Exception):  # Should raise after retries
            connector._fetch_single_page()
    
    def test_rate_limiting(self, sample_api_config):
        """Test rate limiting."""
        config = copy.deepcopy(sample_api_config)
        config['rate_limit'] = {
            'requests_per_second': 10,
            'burst': 20
        }
        
        credentials = {'bearer_token': 'test_token'}
        connector = GenericApiConnector(config, credentials)
        
        start_time = time.time()
        
        # Make 3 requests - should be rate limited
        for _ in range(3):
            connector._apply_rate_limit()
        
        elapsed = time.time() - start_time
        
        # Should take at least 0.2 seconds (3 requests at 10/sec)
        assert elapsed >= 0.15
    
    def test_extract_json_path(self, sample_api_config):
        """Test JSON path extraction."""
        credentials = {'bearer_token': 'test_token'}
        connector = GenericApiConnector(sample_api_config, credentials)
        
        data = {
            'result': {
                'items': [1, 2, 3],
                'meta': {
                    'total': 3
                }
            }
        }
        
        # Test nested path
        items = connector._extract_json_path(data, 'result.items')
        assert items == [1, 2, 3]
        
        # Test deep nested path
        total = connector._extract_json_path(data, 'result.meta.total')
        assert total == 3
        
        # Test invalid path
        result = connector._extract_json_path(data, 'result.nonexistent')
        assert result is None
    
    def test_parse_link_header(self, sample_api_config):
        """Test Link header parsing."""
        credentials = {'bearer_token': 'test_token'}
        connector = GenericApiConnector(sample_api_config, credentials)
        
        link_header = '<https://api.example.com/data?page=2>; rel="next", <https://api.example.com/data?page=10>; rel="last"'
        
        next_url = connector._parse_link_header(link_header, 'next')
        assert next_url == 'https://api.example.com/data?page=2'
        
        last_url = connector._parse_link_header(link_header, 'last')
        assert last_url == 'https://api.example.com/data?page=10'
        
        # Non-existent relation
        prev_url = connector._parse_link_header(link_header, 'prev')
        assert prev_url is None
