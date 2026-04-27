"""
Generic REST API connector with auth, pagination, and retry logic.
Production-ready for Databricks with proper error handling.
"""
import requests
import time
import json
from typing import Dict, Any, List, Optional, Generator, Union
from datetime import datetime
import logging


class GenericApiConnector:
    """Generic connector for REST APIs with auth, pagination, retry."""
    
    def __init__(
        self,
        api_config: Dict[str, Any],
        credentials: Dict[str, str]
    ):
        """
        Initialize the API connector.
        
        Args:
            api_config: API configuration from apis_config.yml
            credentials: Credentials from SecretManager
        """
        self.api_config = api_config
        self.credentials = credentials
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Requests session for connection reuse
        self.session = requests.Session()
        
        # Base configuration
        self.base_url = api_config['base_url']
        self.endpoint = api_config['endpoint']
        self.http_method = api_config.get('http_method', 'GET').upper()
        
        # Authentication
        self.auth_type = api_config.get('auth_type', 'none')
        self._setup_auth()
        
        # Headers
        self.headers = api_config.get('headers', {})
        
        # Pagination
        self.pagination_config = api_config.get('pagination', {})
        
        # Rate limiting
        self.rate_limit_config = api_config.get('rate_limit', {})
        self.last_request_time = 0
        self.request_count = 0
        
        # Retry
        self.retry_config = api_config.get('retry_config', {})
        
        # Timeout
        self.timeout = api_config.get('timeout_seconds', 30)
    
    def _setup_auth(self):
        """Configure authentication based on type."""
        try:
            if self.auth_type == 'bearer':
                token = self.credentials.get('bearer_token')
                if not token:
                    raise ValueError("Bearer token not found in credentials")
                self.session.headers.update({'Authorization': f'Bearer {token}'})
                self.logger.info("Configured Bearer token authentication")
            
            elif self.auth_type == 'api_key':
                api_key = self.credentials.get('api_key')
                if not api_key:
                    raise ValueError("API key not found in credentials")
                
                header_name = self.api_config.get('auth_config', {}).get('api_key_header', 'X-API-Key')
                self.session.headers.update({header_name: api_key})
                self.logger.info(f"Configured API Key authentication ({header_name})")
            
            elif self.auth_type == 'basic':
                username = self.credentials.get('username')
                password = self.credentials.get('password')
                if not username or not password:
                    raise ValueError("Username or password not found for basic auth")
                
                self.session.auth = (username, password)
                self.logger.info("Configured Basic authentication")
            
            elif self.auth_type == 'oauth2_client_credentials':
                # Get OAuth2 token
                token = self._get_oauth2_token()
                self.session.headers.update({'Authorization': f'Bearer {token}'})
                self.logger.info("Configured OAuth2 client credentials authentication")
            
            elif self.auth_type == 'none':
                self.logger.info("No authentication configured")
            
            else:
                raise ValueError(f"Unsupported auth type: {self.auth_type}")
        
        except Exception as e:
            self.logger.error(f"Failed to setup authentication: {str(e)}")
            raise
    
    def _get_oauth2_token(self) -> str:
        """
        Get access token via OAuth2 client credentials flow.
        
        Returns:
            Access token string
        
        Raises:
            ValueError: If token retrieval fails
        """
        auth_config = self.api_config.get('auth_config', {})
        token_url = auth_config.get('token_url')
        
        client_id = self.credentials.get('client_id')
        client_secret = self.credentials.get('client_secret')
        scope = auth_config.get('scope', '')
        
        if not all([token_url, client_id, client_secret]):
            raise ValueError("OAuth2 configuration incomplete (token_url, client_id, client_secret required)")
        
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
        }
        
        if scope:
            data['scope'] = scope
        
        try:
            self.logger.info(f"Requesting OAuth2 token from {token_url}")
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            token_data = response.json()
            
            if 'access_token' not in token_data:
                raise ValueError("No access_token in OAuth2 response")
            
            self.logger.info("OAuth2 token obtained successfully")
            return token_data['access_token']
        
        except Exception as e:
            self.logger.error(f"Failed to obtain OAuth2 token: {str(e)}")
            raise ValueError(f"Failed to obtain OAuth2 token: {str(e)}")
    
    def _apply_rate_limit(self):
        """Apply rate limiting if configured."""
        if not self.rate_limit_config:
            return
        
        requests_per_second = self.rate_limit_config.get('requests_per_second', 10)
        
        # Simple rate limiting
        min_interval = 1.0 / requests_per_second
        
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def _make_request(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """
        Make HTTP request with retry logic.
        
        Args:
            url: Complete URL
            params: Query parameters
        
        Returns:
            Response object
        
        Raises:
            requests.exceptions.RequestException: If all retries fail
        """
        max_retries = self.retry_config.get('max_retries', 3)
        retry_delay = self.retry_config.get('retry_delay_seconds', 5)
        backoff_factor = self.retry_config.get('backoff_factor', 2)
        retry_on_status = self.retry_config.get('retry_on_status', [429, 500, 502, 503, 504])
        
        if params is None:
            params = {}
        
        for attempt in range(max_retries + 1):
            try:
                self._apply_rate_limit()
                
                self.logger.debug(f"Making {self.http_method} request to {url}")
                
                if self.http_method == 'GET':
                    response = self.session.get(
                        url,
                        params=params,
                        headers=self.headers,
                        timeout=self.timeout
                    )
                elif self.http_method == 'POST':
                    response = self.session.post(
                        url,
                        json=params,
                        headers=self.headers,
                        timeout=self.timeout
                    )
                else:
                    raise ValueError(f"Unsupported HTTP method: {self.http_method}")
                
                # If success, return
                if response.status_code == 200:
                    return response
                
                # If non-retriable error, raise immediately
                if response.status_code not in retry_on_status:
                    response.raise_for_status()
                
                # Retriable error - retry if attempts remain
                if attempt < max_retries:
                    wait_time = retry_delay * (backoff_factor ** attempt)
                    self.logger.warning(
                        f"Request failed with status {response.status_code}. "
                        f"Retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                else:
                    # Last attempt failed
                    response.raise_for_status()
            
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    wait_time = retry_delay * (backoff_factor ** attempt)
                    self.logger.warning(
                        f"Request error: {str(e)}. Retrying in {wait_time}s... "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                else:
                    # Last attempt - re-raise
                    self.logger.error(f"All retry attempts exhausted: {str(e)}")
                    raise
        
        raise RuntimeError("Max retries exceeded (should not reach here)")
    
    def fetch_all_data(self) -> List[Dict[str, Any]]:
        """
        Fetch all data from API with pagination.
        
        Returns:
            List of all records
        """
        all_data = []
        page_count = 0
        
        try:
            for page_data in self._paginate():
                all_data.extend(page_data)
                page_count += 1
                
                if page_count % 10 == 0:
                    self.logger.info(f"Fetched {page_count} pages, {len(all_data)} records so far")
            
            self.logger.info(f"Fetch complete: {len(all_data)} total records from {page_count} pages")
            return all_data
        
        except Exception as e:
            self.logger.error(f"Failed to fetch API data: {str(e)}")
            raise
    
    def _paginate(self) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Generator to iterate over all pages.
        
        Yields:
            List of records for each page
        """
        pagination_type = self.pagination_config.get('type', 'none')
        
        self.logger.info(f"Starting pagination (type: {pagination_type})")
        
        if pagination_type == 'none':
            yield self._fetch_single_page()
        
        elif pagination_type == 'offset':
            yield from self._paginate_offset()
        
        elif pagination_type == 'page':
            yield from self._paginate_page()
        
        elif pagination_type == 'cursor':
            yield from self._paginate_cursor()
        
        elif pagination_type == 'link_header':
            yield from self._paginate_link_header()
        
        else:
            raise ValueError(f"Unsupported pagination type: {pagination_type}")
    
    def _fetch_single_page(
        self,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch a single page of data.
        
        Args:
            params: Query parameters (merged with api_config query_params)
        
        Returns:
            List of records
        """
        if params is None:
            params = self.api_config.get('query_params', {}).copy()
        
        url = f"{self.base_url}{self.endpoint}"
        response = self._make_request(url, params)
        
        data = response.json()
        
        # Extract via JSONPath
        response_path = self.api_config.get('response_path')
        if response_path:
            records = self._extract_json_path(data, response_path)
        else:
            records = data if isinstance(data, list) else [data]
        
        if not isinstance(records, list):
            records = [records]
        
        return records
    
    def _paginate_offset(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Pagination by offset."""
        page_size = self.pagination_config.get('page_size', 100)
        page_size_param = self.pagination_config.get('page_size_param', 'limit')
        offset_param = self.pagination_config.get('offset_param', 'offset')
        max_pages = self.pagination_config.get('max_pages')
        
        offset = 0
        page_count = 0
        
        while True:
            params = self.api_config.get('query_params', {}).copy()
            params[page_size_param] = page_size
            params[offset_param] = offset
            
            records = self._fetch_single_page(params)
            
            if not records:
                break
            
            yield records
            
            page_count += 1
            offset += page_size
            
            if max_pages and page_count >= max_pages:
                self.logger.info(f"Reached max_pages limit: {max_pages}")
                break
            
            if len(records) < page_size:
                self.logger.info("Last page reached (fewer records than page_size)")
                break
    
    def _paginate_page(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Pagination by page number."""
        page_size = self.pagination_config.get('page_size', 100)
        page_size_param = self.pagination_config.get('page_size_param', 'per_page')
        page_param = self.pagination_config.get('page_param', 'page')
        max_pages = self.pagination_config.get('max_pages')
        
        page = 1
        
        while True:
            params = self.api_config.get('query_params', {}).copy()
            params[page_size_param] = page_size
            params[page_param] = page
            
            records = self._fetch_single_page(params)
            
            if not records:
                break
            
            yield records
            
            page += 1
            
            if max_pages and page > max_pages:
                self.logger.info(f"Reached max_pages limit: {max_pages}")
                break
            
            if len(records) < page_size:
                self.logger.info("Last page reached")
                break
    
    def _paginate_cursor(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Pagination by cursor."""
        page_size = self.pagination_config.get('page_size', 100)
        page_size_param = self.pagination_config.get('page_size_param', 'limit')
        cursor_param = self.pagination_config.get('cursor_param', 'cursor')
        cursor_response_path = self.pagination_config.get('cursor_response_path', 'next_cursor')
        max_pages = self.pagination_config.get('max_pages')
        
        cursor = None
        page_count = 0
        
        while True:
            params = self.api_config.get('query_params', {}).copy()
            params[page_size_param] = page_size
            
            if cursor:
                params[cursor_param] = cursor
            
            url = f"{self.base_url}{self.endpoint}"
            response = self._make_request(url, params)
            data = response.json()
            
            # Extract records
            response_path = self.api_config.get('response_path')
            records = self._extract_json_path(data, response_path) if response_path else data
            
            if not isinstance(records, list):
                records = [records] if records else []
            
            if not records:
                break
            
            yield records
            
            page_count += 1
            
            # Get next cursor
            cursor = self._extract_json_path(data, cursor_response_path)
            
            if not cursor:
                self.logger.info("No next cursor - pagination complete")
                break
            
            if max_pages and page_count >= max_pages:
                self.logger.info(f"Reached max_pages limit: {max_pages}")
                break
    
    def _paginate_link_header(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Pagination via Link header (RFC 5988)."""
        page_size = self.pagination_config.get('page_size', 100)
        page_size_param = self.pagination_config.get('page_size_param', 'per_page')
        max_pages = self.pagination_config.get('max_pages')
        
        params = self.api_config.get('query_params', {}).copy()
        params[page_size_param] = page_size
        
        url = f"{self.base_url}{self.endpoint}"
        page_count = 0
        
        while url:
            response = self._make_request(url, params)
            data = response.json()
            
            response_path = self.api_config.get('response_path')
            records = self._extract_json_path(data, response_path) if response_path else data
            
            if not isinstance(records, list):
                records = [records] if records else []
            
            if records:
                yield records
            
            page_count += 1
            
            if max_pages and page_count >= max_pages:
                self.logger.info(f"Reached max_pages limit: {max_pages}")
                break
            
            # Get next URL from Link header
            link_header = response.headers.get('Link', '')
            url = self._parse_link_header(link_header, 'next')
            params = {}  # Params are in URL for subsequent requests
    
    def _extract_json_path(
        self,
        data: Any,
        path: str
    ) -> Any:
        """
        Extract value from JSON via dot-separated path.
        
        Args:
            data: JSON data
            path: Dot-separated path (e.g., 'data.customers')
        
        Returns:
            Extracted value or None
        """
        if not path:
            return data
        
        keys = path.split('.')
        current = data
        
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
                if current is None:
                    return None
            else:
                return None
        
        return current
    
    def _parse_link_header(
        self,
        link_header: str,
        rel: str
    ) -> Optional[str]:
        """
        Parse Link header (RFC 5988) to find specific relation.
        
        Args:
            link_header: Link header string
            rel: Relation to find (e.g., 'next')
        
        Returns:
            URL or None
        """
        if not link_header:
            return None
        
        links = link_header.split(',')
        for link in links:
            parts = link.split(';')
            if len(parts) < 2:
                continue
            
            url = parts[0].strip().strip('<>')
            rel_part = parts[1].strip()
            
            if f'rel="{rel}"' in rel_part or f"rel='{rel}'" in rel_part:
                return url
        
        return None
