"""
Centralized configuration management for F&O AI Trader.
"""
import os
import yaml
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path
from config.logging_config import get_logger

logger = get_logger('config')


@dataclass
class DatabaseConfig:
    """Database configuration."""
    path: str = "database/market_data.db"
    connection_timeout: int = 30
    max_retries: int = 3


@dataclass
class APIConfig:
    """API configuration settings."""
    nse_rate_limit: int = 30  # calls per minute
    telegram_rate_limit: int = 20  # calls per minute
    yfinance_rate_limit: int = 60  # calls per minute
    request_timeout: int = 10  # seconds
    max_retries: int = 3
    retry_delay: float = 1.0  # seconds


@dataclass
class TradingConfig:
    """Trading strategy configuration."""
    default_symbol: str = "NIFTY"
    default_ticker: str = "^NSEI"
    default_interval: str = "5m"
    default_period: str = "30d"
    prediction_horizon: int = 1  # periods ahead to predict
    features: list = field(default_factory=lambda: ["ema9", "ema21", "rsi", "atr", "vwap"])
    model_retrain_days: int = 7  # days before retraining model
    signal_confidence_threshold: float = 0.6  # minimum confidence for signals


@dataclass
class ModelConfig:
    """Machine learning model configuration."""
    model_type: str = "xgboost"
    train_ratio: float = 0.7
    val_ratio: float = 0.15
    test_ratio: float = 0.15
    random_state: int = 42
    n_estimators: int = 200
    max_depth: int = 5
    learning_rate: float = 0.1
    early_stopping_rounds: int = 20


@dataclass
class DataConfig:
    """Data processing configuration."""
    raw_data_dir: str = "data/raw"
    processed_data_dir: str = "data/processed"
    max_file_age_hours: int = 24  # Maximum age of data files
    data_quality_check_interval: int = 60  # minutes
    missing_data_threshold: float = 0.2  # 20% missing data threshold
    price_change_alert_threshold: float = 0.1  # 10% price change alert


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    log_to_file: bool = True
    log_to_console: bool = True
    log_dir: str = "logs"
    max_file_size_mb: int = 10
    backup_count: int = 5


@dataclass
class AppConfig:
    """Main application configuration."""
    environment: str = "development"  # development, staging, production
    debug: bool = False
    port: int = 8080
    host: str = "localhost"
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    api: APIConfig = field(default_factory=APIConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    data: DataConfig = field(default_factory=DataConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


class ConfigManager:
    """Configuration manager for loading and managing application config."""

    def __init__(self, config_file: str = "config/settings.yaml"):
        self.config_file = config_file
        self._config: Optional[AppConfig] = None
        self._load_config()

    def _load_config(self):
        """Load configuration from YAML file and environment variables."""
        try:
            # Start with default configuration
            config_dict = {}

            # Load from YAML file if it exists
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    yaml_config = yaml.safe_load(f) or {}
                    config_dict.update(yaml_config)
                    logger.info(f"Loaded configuration from {self.config_file}")

            # Override with environment variables
            self._load_env_overrides(config_dict)

            # Create configuration object
            self._config = self._dict_to_config(config_dict)

            # Environment-specific adjustments
            self._apply_environment_settings()

            # Re-apply environment variable overrides to ensure they take precedence
            # over environment-specific settings
            env_overrides = {}
            self._load_env_overrides(env_overrides)
            if env_overrides:
                # Apply any environment overrides on top of the final config
                override_config = self._dict_to_config({**config_dict, **env_overrides})
                # Merge the override values back into the main config
                self._merge_config_overrides(override_config)

            logger.info(f"Configuration loaded for environment: {self._config.environment}")

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            # Fall back to default configuration
            self._config = AppConfig()

    def _load_env_overrides(self, config_dict: Dict[str, Any]):
        """Load configuration overrides from environment variables."""
        env_mappings = {
            'TRADING_ENV': 'environment',
            'TRADING_DEBUG': 'debug',
            'TRADING_PORT': 'port',
            'TRADING_HOST': 'host',
            'DB_PATH': 'database.path',
            'LOG_LEVEL': 'logging.level',
            'MODEL_RETRAIN_DAYS': 'trading.model_retrain_days',
            'NSE_RATE_LIMIT': 'api.nse_rate_limit',
            'API_TIMEOUT': 'api.request_timeout',
            'SIGNAL_THRESHOLD': 'trading.signal_confidence_threshold'
        }

        for env_var, config_path in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                self._set_nested_value(config_dict, config_path, env_value)
                logger.info(f"Environment override: {env_var} -> {config_path}")

    def _set_nested_value(self, config_dict: Dict[str, Any], path: str, value: str):
        """Set a nested configuration value using dot notation."""
        keys = path.split('.')
        current = config_dict

        # Navigate to the parent dictionary
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        # Convert value to appropriate type
        final_key = keys[-1]

        # Get the default value to determine the expected type
        default_value = current.get(final_key)

        try:
            if isinstance(default_value, bool):
                value = value.lower() in ('true', '1', 'yes', 'on')
            elif isinstance(default_value, int):
                value = int(value)
            elif isinstance(default_value, float):
                value = float(value)
            # If no default value exists, try to infer type from common field names and values
            elif default_value is None:
                # Check for boolean field names or boolean-like values
                if (final_key.lower() in ('debug', 'enabled', 'active', 'log_to_file', 'log_to_console') or
                    value.lower() in ('true', 'false', '1', '0', 'yes', 'no', 'on', 'off')):
                    value = value.lower() in ('true', '1', 'yes', 'on')
                else:
                    # Try int first, then float, then keep as string
                    try:
                        value = int(value)
                    except ValueError:
                        try:
                            value = float(value)
                        except ValueError:
                            # Keep as string
                            pass
        except (ValueError, TypeError):
            # If conversion fails, keep as string
            pass

        current[final_key] = value

    def _merge_config_overrides(self, override_config: AppConfig):
        """Merge environment variable overrides into the main config."""
        # Only override fields that were explicitly set via environment variables
        env_mappings = {
            'TRADING_ENV': 'environment',
            'TRADING_DEBUG': 'debug',
            'TRADING_PORT': 'port',
            'TRADING_HOST': 'host',
            'DB_PATH': 'database.path',
            'LOG_LEVEL': 'logging.level',
            'MODEL_RETRAIN_DAYS': 'trading.model_retrain_days',
            'NSE_RATE_LIMIT': 'api.nse_rate_limit',
            'API_TIMEOUT': 'api.request_timeout',
            'SIGNAL_THRESHOLD': 'trading.signal_confidence_threshold'
        }

        for env_var, config_path in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Override the specific field
                if config_path == 'debug':
                    self._config.debug = override_config.debug
                elif config_path == 'port':
                    self._config.port = override_config.port
                elif config_path == 'host':
                    self._config.host = override_config.host
                elif config_path == 'environment':
                    self._config.environment = override_config.environment
                elif config_path == 'database.path':
                    self._config.database.path = override_config.database.path
                elif config_path == 'logging.level':
                    self._config.logging.level = override_config.logging.level

    def _dict_to_config(self, config_dict: Dict[str, Any]) -> AppConfig:
        """Convert configuration dictionary to AppConfig object."""
        # Helper function to create dataclass from dict
        def create_dataclass(cls, data):
            if not isinstance(data, dict):
                return data

            # Get field names and types for the dataclass
            field_types = {f.name: f.type for f in cls.__dataclass_fields__.values()}
            kwargs = {}

            for field_name, field_type in field_types.items():
                if field_name in data:
                    value = data[field_name]
                    # If the field type is also a dataclass, recursively create it
                    if hasattr(field_type, '__dataclass_fields__'):
                        kwargs[field_name] = create_dataclass(field_type, value)
                    else:
                        kwargs[field_name] = value

            return cls(**kwargs)

        return create_dataclass(AppConfig, config_dict)

    def _apply_environment_settings(self):
        """Apply environment-specific configuration settings."""
        if self._config.environment == "production":
            self._config.debug = False
            self._config.logging.level = "INFO"
            self._config.logging.log_to_console = False
            self._config.api.max_retries = 5
            self._config.model.early_stopping_rounds = 50

        elif self._config.environment == "development":
            self._config.debug = True
            self._config.logging.level = "DEBUG"
            self._config.logging.log_to_console = True
            self._config.api.max_retries = 2

        elif self._config.environment == "staging":
            self._config.debug = False
            self._config.logging.level = "INFO"
            self._config.api.max_retries = 3

    def get_config(self) -> AppConfig:
        """Get the current configuration."""
        return self._config

    def reload_config(self):
        """Reload configuration from file."""
        logger.info("Reloading configuration...")
        self._load_config()

    def save_config_template(self, template_path: str = "config/settings.yaml.template"):
        """Save a configuration template file."""
        template_config = {
            'environment': 'development',
            'debug': True,
            'port': 8080,
            'host': 'localhost',
            'database': {
                'path': 'database/market_data.db',
                'connection_timeout': 30,
                'max_retries': 3
            },
            'api': {
                'nse_rate_limit': 30,
                'telegram_rate_limit': 20,
                'yfinance_rate_limit': 60,
                'request_timeout': 10,
                'max_retries': 3,
                'retry_delay': 1.0
            },
            'trading': {
                'default_symbol': 'NIFTY',
                'default_ticker': '^NSEI',
                'default_interval': '5m',
                'default_period': '30d',
                'prediction_horizon': 1,
                'features': ['ema9', 'ema21', 'rsi', 'atr', 'vwap'],
                'model_retrain_days': 7,
                'signal_confidence_threshold': 0.6
            },
            'model': {
                'model_type': 'xgboost',
                'train_ratio': 0.7,
                'val_ratio': 0.15,
                'test_ratio': 0.15,
                'random_state': 42,
                'n_estimators': 200,
                'max_depth': 5,
                'learning_rate': 0.1,
                'early_stopping_rounds': 20
            },
            'data': {
                'raw_data_dir': 'data/raw',
                'processed_data_dir': 'data/processed',
                'max_file_age_hours': 24,
                'data_quality_check_interval': 60,
                'missing_data_threshold': 0.2,
                'price_change_alert_threshold': 0.1
            },
            'logging': {
                'level': 'INFO',
                'log_to_file': True,
                'log_to_console': True,
                'log_dir': 'logs',
                'max_file_size_mb': 10,
                'backup_count': 5
            }
        }

        os.makedirs(os.path.dirname(template_path), exist_ok=True)
        with open(template_path, 'w') as f:
            yaml.dump(template_config, f, default_flow_style=False, indent=2)

        logger.info(f"Configuration template saved to {template_path}")


# Global configuration manager instance
_config_manager = None

def get_config() -> AppConfig:
    """Get the global configuration instance."""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager.get_config()

def reload_config():
    """Reload the global configuration."""
    global _config_manager
    if _config_manager:
        _config_manager.reload_config()

def get_config_manager() -> ConfigManager:
    """Get the configuration manager instance."""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager