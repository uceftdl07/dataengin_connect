"""
Unit tests for ConfigLoader.
"""
import pytest
from src.utils.config_loader import ConfigLoader


@pytest.mark.unit
class TestConfigLoader:
    """Test ConfigLoader class."""

    def test_init_valid_domain(self, tmp_path, temp_yaml_files):
        project_root = tmp_path
        loader = ConfigLoader(project_root, 'test_domain')
    
    def test_init_invalid_domain(self, tmp_path):
        """Test initialization with invalid domain raises error."""
        with pytest.raises(ValueError, match="Domain path not found"):
            ConfigLoader(tmp_path, 'nonexistent_domain')
    
    def test_load_tables_config(self, tmp_path, temp_yaml_files):
        """Test loading tables configuration."""
        loader = ConfigLoader(tmp_path, 'test_domain')
        config = loader.load_tables_config()
        
        assert config['domain'] == 'test_domain'
        assert 'tables' in config
        assert len(config['tables']) == 1
        assert config['tables'][0]['table_name'] == 'dbo.TestTable'
    
    def test_load_tables_config_missing_file(self, tmp_path):
        """Test loading tables config when file doesn't exist."""
        # Create domain dir but no files
        domain_dir = tmp_path / "config" / "domains" / "empty_domain"
        domain_dir.mkdir(parents=True)
        
        loader = ConfigLoader(tmp_path, 'empty_domain')
        
        with pytest.raises(FileNotFoundError, match="Tables config not found"):
            loader.load_tables_config()
    
    def test_load_apis_config(self, tmp_path, temp_yaml_files):
        """Test loading APIs configuration."""
        loader = ConfigLoader(tmp_path, 'test_domain')
        config = loader.load_apis_config()
        
        assert config['domain'] == 'test_domain'
        assert 'apis' in config
        assert len(config['apis']) == 1
        assert config['apis'][0]['api_name'] == 'test_api'
    
    def test_load_connector_config(self, tmp_path, temp_yaml_files):
        """Test loading connector configuration."""
        loader = ConfigLoader(tmp_path, 'test_domain')
        config = loader.load_connector_config()
        
        assert config['domain'] == 'test_domain'
        assert 'connector' in config
        assert config['connector']['type'] == 'sqlserver'
    
    def test_get_enabled_tables(self, tmp_path, temp_yaml_files):
        """Test getting only enabled tables."""
        loader = ConfigLoader(tmp_path, 'test_domain')
        tables = loader.get_enabled_tables()
        
        assert len(tables) == 1
        assert tables[0]['enabled'] is True
    
    def test_get_enabled_apis(self, tmp_path, temp_yaml_files):
        """Test getting only enabled APIs."""
        loader = ConfigLoader(tmp_path, 'test_domain')
        apis = loader.get_enabled_apis()
        
        assert len(apis) == 1
        assert apis[0]['enabled'] is True
    
    def test_get_global_table_options(self, tmp_path, temp_yaml_files):
        """Test getting global table options."""
        loader = ConfigLoader(tmp_path, 'test_domain')
        options = loader.get_global_table_options()
        
        assert 'add_metadata_columns' in options
        assert options['add_metadata_columns'] is True
    
    def test_get_global_api_options(self, tmp_path, temp_yaml_files):
        """Test getting global API options."""
        loader = ConfigLoader(tmp_path, 'test_domain')
        options = loader.get_global_api_options()
        
        assert isinstance(options, dict)
    
    def test_domain_mismatch_tables(self, tmp_path, temp_yaml_files):
        """Test error when domain name doesn't match in YAML."""
        import yaml
        
        # Create config with wrong domain
        wrong_config = {
            'domain': 'wrong_domain',
            'tables': []
        }
        
        config_file = temp_yaml_files / 'tables_config.yml'
        with open(config_file, 'w') as f:
            yaml.dump(wrong_config, f)
        
        loader = ConfigLoader(tmp_path, 'test_domain')
        
        with pytest.raises(ValueError, match="Domain mismatch"):
            loader.load_tables_config()
    
    def test_empty_config_file(self, tmp_path, temp_yaml_files):
        """Test error when config file is empty."""
        config_file = temp_yaml_files / 'tables_config.yml'
        with open(config_file, 'w') as f:
            f.write('')
        
        loader = ConfigLoader(tmp_path, 'test_domain')
        
        with pytest.raises(ValueError, match="Empty configuration"):
            loader.load_tables_config()
    
    def test_missing_tables_section(self, tmp_path, temp_yaml_files):
        """Test error when 'tables' section is missing."""
        import yaml
        
        wrong_config = {
            'domain': 'test_domain',
            # Missing 'tables' key
        }
        
        config_file = temp_yaml_files / 'tables_config.yml'
        with open(config_file, 'w') as f:
            yaml.dump(wrong_config, f)
        
        loader = ConfigLoader(tmp_path, 'test_domain')
        
        with pytest.raises(ValueError, match="No 'tables' section"):
            loader.load_tables_config()
