"""
Tests for config.py - Configuration loading
"""

import os
import pytest
import tempfile

os.environ["XDG_DATA_HOME"] = tempfile.mkdtemp()

from avm.config import AVMConfig, ProviderSpec, PermissionRule, load_config


class TestAVMConfig:
    """Tests for AVMConfig."""
    
    def test_default_config(self):
        """Test default configuration."""
        config = AVMConfig()
        assert isinstance(config.providers, list)
        assert isinstance(config.permissions, list)
    
    def test_config_with_providers(self):
        """Test config with providers."""
        spec = ProviderSpec(
            pattern="/test/**",
            type="file",
            config={"root": "/tmp"}
        )
        config = AVMConfig(providers=[spec])
        
        assert len(config.providers) == 1
        assert config.providers[0].pattern == "/test/**"


class TestProviderSpec:
    """Tests for ProviderSpec."""
    
    def test_provider_spec_creation(self):
        """Test creating a ProviderSpec."""
        spec = ProviderSpec(
            pattern="/api/**",
            type="http",
            config={"base_url": "http://localhost"}
        )
        
        assert spec.pattern == "/api/**"
        assert spec.type == "http"
        assert spec.config["base_url"] == "http://localhost"
    
    def test_provider_spec_matches(self):
        """Test pattern matching."""
        spec = ProviderSpec(pattern="/files/*", type="file")
        
        assert spec.matches("/files/test.txt")
        assert not spec.matches("/other/file.txt")
    
    def test_provider_spec_with_ttl(self):
        """Test TTL configuration."""
        spec = ProviderSpec(pattern="/cache/**", type="http", ttl=60)
        assert spec.ttl == 60


class TestPermissionRule:
    """Tests for PermissionRule."""
    
    def test_permission_rule_creation(self):
        """Test creating a PermissionRule."""
        rule = PermissionRule(pattern="/private/**", access="ro")
        
        assert rule.pattern == "/private/**"
        assert rule.access == "ro"
        assert rule.can_read is True
        assert rule.can_write is False
    
    def test_permission_rule_rw(self):
        """Test read-write permission."""
        rule = PermissionRule(pattern="/public/**", access="rw")
        
        assert rule.can_read is True
        assert rule.can_write is True
    
    def test_permission_rule_none(self):
        """Test no access permission."""
        rule = PermissionRule(pattern="/secret/**", access="none")
        
        assert rule.can_read is False
        assert rule.can_write is False
    
    def test_permission_rule_matches(self):
        """Test pattern matching."""
        rule = PermissionRule(pattern="/data/*", access="ro")
        
        assert rule.matches("/data/file.txt")
        assert not rule.matches("/other/file.txt")


class TestLoadConfig:
    """Tests for load_config function."""
    
    def test_load_yaml_config(self, tmp_path):
        """Test loading YAML config file."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
db_path: /tmp/avm.db
providers:
  - pattern: "/test/**"
    type: file
    config:
      root: /tmp
permissions:
  - pattern: "/**"
    access: ro
""")
        
        config = load_config(str(config_file))
        
        assert config.db_path == "/tmp/avm.db"
        assert len(config.providers) >= 1
    
    def test_load_nonexistent_returns_default(self):
        """Test loading nonexistent file returns default config."""
        config = load_config("/nonexistent/config.yaml")
        
        # Should return default config
        assert config is not None
        assert isinstance(config, AVMConfig)
