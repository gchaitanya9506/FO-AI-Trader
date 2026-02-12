"""
Unit tests for configuration management.
"""
import unittest
import tempfile
import os
import yaml
import sys
from unittest.mock import patch

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.app_config import (
    AppConfig,
    ConfigManager,
    DatabaseConfig,
    APIConfig,
    TradingConfig,
    ModelConfig,
    DataConfig,
    LoggingConfig
)


class TestConfigurationClasses(unittest.TestCase):
    """Test configuration dataclasses."""

    def test_default_database_config(self):
        """Test default database configuration."""
        config = DatabaseConfig()
        self.assertEqual(config.path, "database/market_data.db")
        self.assertEqual(config.connection_timeout, 30)
        self.assertEqual(config.max_retries, 3)

    def test_default_api_config(self):
        """Test default API configuration."""
        config = APIConfig()
        self.assertEqual(config.nse_rate_limit, 30)
        self.assertEqual(config.telegram_rate_limit, 20)
        self.assertEqual(config.yfinance_rate_limit, 60)
        self.assertEqual(config.request_timeout, 10)
        self.assertEqual(config.max_retries, 3)
        self.assertEqual(config.retry_delay, 1.0)

    def test_default_trading_config(self):
        """Test default trading configuration."""
        config = TradingConfig()
        self.assertEqual(config.default_symbol, "NIFTY")
        self.assertEqual(config.default_ticker, "^NSEI")
        self.assertEqual(config.default_interval, "5m")
        self.assertEqual(config.default_period, "30d")
        self.assertEqual(config.prediction_horizon, 1)
        self.assertIn("ema9", config.features)
        self.assertIn("rsi", config.features)
        self.assertEqual(config.signal_confidence_threshold, 0.6)

    def test_default_model_config(self):
        """Test default model configuration."""
        config = ModelConfig()
        self.assertEqual(config.model_type, "xgboost")
        self.assertEqual(config.train_ratio, 0.7)
        self.assertEqual(config.val_ratio, 0.15)
        self.assertEqual(config.test_ratio, 0.15)
        self.assertEqual(config.random_state, 42)
        self.assertEqual(config.n_estimators, 200)

    def test_default_app_config(self):
        """Test default application configuration."""
        config = AppConfig()
        self.assertEqual(config.environment, "development")
        self.assertFalse(config.debug)
        self.assertEqual(config.port, 8080)
        self.assertEqual(config.host, "localhost")

        # Check nested configurations
        self.assertIsInstance(config.database, DatabaseConfig)
        self.assertIsInstance(config.api, APIConfig)
        self.assertIsInstance(config.trading, TradingConfig)
        self.assertIsInstance(config.model, ModelConfig)


class TestConfigManager(unittest.TestCase):
    """Test configuration manager."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_config_file = os.path.join(self.temp_dir, "test_settings.yaml")

    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)

    def test_load_default_config(self):
        """Test loading default configuration when no file exists."""
        non_existent_file = os.path.join(self.temp_dir, "nonexistent.yaml")
        config_manager = ConfigManager(non_existent_file)
        config = config_manager.get_config()

        self.assertIsInstance(config, AppConfig)
        self.assertEqual(config.environment, "development")

    def test_load_config_from_yaml(self):
        """Test loading configuration from YAML file."""
        # Create test config file
        test_config = {
            'environment': 'testing',
            'debug': True,
            'port': 9000,
            'api': {
                'request_timeout': 20,
                'max_retries': 5
            },
            'trading': {
                'default_symbol': 'BANKNIFTY',
                'features': ['ema9', 'rsi']
            }
        }

        with open(self.test_config_file, 'w') as f:
            yaml.dump(test_config, f)

        config_manager = ConfigManager(self.test_config_file)
        config = config_manager.get_config()

        self.assertEqual(config.environment, 'testing')
        self.assertTrue(config.debug)
        self.assertEqual(config.port, 9000)
        self.assertEqual(config.api.request_timeout, 20)
        self.assertEqual(config.api.max_retries, 5)
        self.assertEqual(config.trading.default_symbol, 'BANKNIFTY')
        self.assertEqual(config.trading.features, ['ema9', 'rsi'])

    def test_environment_variable_overrides(self):
        """Test configuration overrides from environment variables."""
        with patch.dict(os.environ, {
            'TRADING_ENV': 'production',
            'TRADING_PORT': '8888',
            'TRADING_DEBUG': 'true',
            'DB_PATH': 'test/path.db',
            'LOG_LEVEL': 'ERROR'
        }):
            config_manager = ConfigManager(self.test_config_file)
            config = config_manager.get_config()

            self.assertEqual(config.environment, 'production')
            self.assertEqual(config.port, 8888)
            self.assertTrue(config.debug)
            self.assertEqual(config.database.path, 'test/path.db')
            self.assertEqual(config.logging.level, 'ERROR')

    def test_production_environment_settings(self):
        """Test production environment specific settings."""
        test_config = {'environment': 'production'}

        with open(self.test_config_file, 'w') as f:
            yaml.dump(test_config, f)

        config_manager = ConfigManager(self.test_config_file)
        config = config_manager.get_config()

        self.assertEqual(config.environment, 'production')
        self.assertFalse(config.debug)
        self.assertEqual(config.logging.level, 'INFO')
        self.assertFalse(config.logging.log_to_console)
        self.assertEqual(config.api.max_retries, 5)

    def test_development_environment_settings(self):
        """Test development environment specific settings."""
        test_config = {'environment': 'development'}

        with open(self.test_config_file, 'w') as f:
            yaml.dump(test_config, f)

        config_manager = ConfigManager(self.test_config_file)
        config = config_manager.get_config()

        self.assertEqual(config.environment, 'development')
        self.assertTrue(config.debug)
        self.assertEqual(config.logging.level, 'DEBUG')
        self.assertTrue(config.logging.log_to_console)

    def test_nested_config_override(self):
        """Test overriding nested configuration values."""
        config_manager = ConfigManager(self.test_config_file)

        # Test setting nested value
        config_dict = {}
        config_manager._set_nested_value(config_dict, 'database.connection_timeout', '60')

        self.assertEqual(config_dict['database']['connection_timeout'], 60)

    def test_config_reload(self):
        """Test configuration reloading."""
        # Create initial config
        initial_config = {'port': 8080}
        with open(self.test_config_file, 'w') as f:
            yaml.dump(initial_config, f)

        config_manager = ConfigManager(self.test_config_file)
        config = config_manager.get_config()
        self.assertEqual(config.port, 8080)

        # Update config file
        updated_config = {'port': 9999}
        with open(self.test_config_file, 'w') as f:
            yaml.dump(updated_config, f)

        # Reload and check
        config_manager.reload_config()
        config = config_manager.get_config()
        self.assertEqual(config.port, 9999)

    def test_save_config_template(self):
        """Test saving configuration template."""
        config_manager = ConfigManager(self.test_config_file)
        template_path = os.path.join(self.temp_dir, "template.yaml")

        config_manager.save_config_template(template_path)

        self.assertTrue(os.path.exists(template_path))

        # Load and verify template
        with open(template_path, 'r') as f:
            template_data = yaml.safe_load(f)

        self.assertIn('environment', template_data)
        self.assertIn('database', template_data)
        self.assertIn('api', template_data)
        self.assertIn('trading', template_data)


class TestConfigurationValidation(unittest.TestCase):
    """Test configuration validation logic."""

    def test_model_ratios_sum_to_one(self):
        """Test that model ratios sum to 1.0."""
        config = ModelConfig()
        total = config.train_ratio + config.val_ratio + config.test_ratio
        self.assertAlmostEqual(total, 1.0, places=6)

    def test_valid_port_range(self):
        """Test port number validation."""
        config = AppConfig()
        self.assertGreater(config.port, 0)
        self.assertLess(config.port, 65536)

    def test_positive_timeouts(self):
        """Test that timeout values are positive."""
        config = APIConfig()
        self.assertGreater(config.request_timeout, 0)
        self.assertGreaterEqual(config.max_retries, 0)
        self.assertGreater(config.retry_delay, 0)

    def test_features_list_not_empty(self):
        """Test that features list is not empty."""
        config = TradingConfig()
        self.assertIsInstance(config.features, list)
        self.assertGreater(len(config.features), 0)

    def test_confidence_threshold_range(self):
        """Test that confidence threshold is in valid range."""
        config = TradingConfig()
        self.assertGreaterEqual(config.signal_confidence_threshold, 0.0)
        self.assertLessEqual(config.signal_confidence_threshold, 1.0)


class TestEnvironmentSpecificSettings(unittest.TestCase):
    """Test environment-specific configuration settings."""

    def test_production_security_settings(self):
        """Test that production has secure defaults."""
        config_manager = ConfigManager()
        config_manager._config = AppConfig()
        config_manager._config.environment = "production"
        config_manager._apply_environment_settings()

        config = config_manager._config

        # Production should be secure
        self.assertFalse(config.debug)
        self.assertEqual(config.logging.level, "INFO")
        self.assertFalse(config.logging.log_to_console)

    def test_development_convenience_settings(self):
        """Test that development has convenient defaults."""
        config_manager = ConfigManager()
        config_manager._config = AppConfig()
        config_manager._config.environment = "development"
        config_manager._apply_environment_settings()

        config = config_manager._config

        # Development should be convenient for debugging
        self.assertTrue(config.debug)
        self.assertEqual(config.logging.level, "DEBUG")
        self.assertTrue(config.logging.log_to_console)


if __name__ == '__main__':
    unittest.main()