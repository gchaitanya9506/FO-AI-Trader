#!/usr/bin/env python3
"""
Configuration validation script for F&O AI Trader.
Validates all configuration settings and environment setup.
"""
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.app_config import get_config, get_config_manager
from config.secrets import validate_environment
from config.logging_config import get_logger

logger = get_logger('config_check')


def check_environment_variables():
    """Check that all required environment variables are set."""
    logger.info("Checking environment variables...")

    try:
        validate_environment()
        logger.info("✓ All required environment variables are set")
        return True
    except EnvironmentError as e:
        logger.error(f"✗ Environment variable validation failed: {e}")
        return False


def check_configuration_structure():
    """Validate configuration structure and values."""
    logger.info("Checking configuration structure...")

    try:
        config = get_config()

        # Check basic configuration structure
        assert hasattr(config, 'environment'), "Missing environment setting"
        assert hasattr(config, 'database'), "Missing database configuration"
        assert hasattr(config, 'api'), "Missing API configuration"
        assert hasattr(config, 'trading'), "Missing trading configuration"
        assert hasattr(config, 'model'), "Missing model configuration"
        assert hasattr(config, 'data'), "Missing data configuration"
        assert hasattr(config, 'logging'), "Missing logging configuration"

        # Validate environment
        valid_environments = ['development', 'staging', 'production']
        assert config.environment in valid_environments, f"Invalid environment: {config.environment}"

        # Validate numeric ranges
        assert 1 <= config.port <= 65535, f"Invalid port: {config.port}"
        assert config.api.request_timeout > 0, "Request timeout must be positive"
        assert config.api.max_retries >= 0, "Max retries cannot be negative"
        assert 0 < config.model.train_ratio < 1, "Train ratio must be between 0 and 1"
        assert 0 < config.model.val_ratio < 1, "Validation ratio must be between 0 and 1"
        assert 0 < config.model.test_ratio < 1, "Test ratio must be between 0 and 1"

        # Validate ratios sum to 1
        total_ratio = config.model.train_ratio + config.model.val_ratio + config.model.test_ratio
        assert abs(total_ratio - 1.0) < 0.001, f"Model ratios must sum to 1.0, got {total_ratio}"

        # Validate features list
        assert isinstance(config.trading.features, list), "Trading features must be a list"
        assert len(config.trading.features) > 0, "Trading features list cannot be empty"

        logger.info("✓ Configuration structure is valid")
        return True

    except Exception as e:
        logger.error(f"✗ Configuration validation failed: {e}")
        return False


def check_directory_structure():
    """Check that required directories exist or can be created."""
    logger.info("Checking directory structure...")

    try:
        config = get_config()

        directories_to_check = [
            config.data.raw_data_dir,
            config.data.processed_data_dir,
            config.logging.log_dir,
            os.path.dirname(config.database.path),
            "models/saved_models"
        ]

        for directory in directories_to_check:
            if not os.path.exists(directory):
                try:
                    os.makedirs(directory, exist_ok=True)
                    logger.info(f"✓ Created directory: {directory}")
                except Exception as e:
                    logger.error(f"✗ Cannot create directory {directory}: {e}")
                    return False
            else:
                logger.info(f"✓ Directory exists: {directory}")

        logger.info("✓ Directory structure is valid")
        return True

    except Exception as e:
        logger.error(f"✗ Directory structure check failed: {e}")
        return False


def check_file_permissions():
    """Check file permissions for critical paths."""
    logger.info("Checking file permissions...")

    try:
        config = get_config()

        # Check if we can write to critical directories
        test_paths = [
            config.data.raw_data_dir,
            config.data.processed_data_dir,
            config.logging.log_dir,
            os.path.dirname(config.database.path)
        ]

        for path in test_paths:
            test_file = os.path.join(path, '.permission_test')
            try:
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
                logger.info(f"✓ Write permission confirmed for: {path}")
            except Exception as e:
                logger.error(f"✗ No write permission for {path}: {e}")
                return False

        logger.info("✓ File permissions are adequate")
        return True

    except Exception as e:
        logger.error(f"✗ File permissions check failed: {e}")
        return False


def check_python_dependencies():
    """Check that required Python packages are available."""
    logger.info("Checking Python dependencies...")

    required_packages = [
        'pandas', 'xgboost', 'joblib', 'yfinance',
        'requests', 'flask', 'ta', 'scipy', 'pyyaml'
    ]

    optional_packages = ['nsepython', 'nsepy']

    missing_required = []
    missing_optional = []

    for package in required_packages:
        try:
            __import__(package)
            logger.info(f"✓ Required package available: {package}")
        except ImportError:
            logger.error(f"✗ Missing required package: {package}")
            missing_required.append(package)

    for package in optional_packages:
        try:
            __import__(package)
            logger.info(f"✓ Optional package available: {package}")
        except ImportError:
            logger.warning(f"⚠ Missing optional package: {package}")
            missing_optional.append(package)

    if missing_required:
        logger.error(f"Missing required packages: {missing_required}")
        logger.error("Install with: pip install " + " ".join(missing_required))
        return False

    if missing_optional:
        logger.warning(f"Missing optional packages: {missing_optional}")
        logger.warning("Some features may not work without optional packages")

    logger.info("✓ Python dependencies check completed")
    return True


def generate_configuration_report():
    """Generate a comprehensive configuration report."""
    config = get_config()

    report = f"""
=== F&O AI TRADER CONFIGURATION REPORT ===

Environment: {config.environment}
Debug Mode: {config.debug}
Application Host: {config.host}
Application Port: {config.port}

Database:
  Path: {config.database.path}
  Timeout: {config.database.connection_timeout}s
  Max Retries: {config.database.max_retries}

API Configuration:
  NSE Rate Limit: {config.api.nse_rate_limit} calls/min
  Telegram Rate Limit: {config.api.telegram_rate_limit} calls/min
  YFinance Rate Limit: {config.api.yfinance_rate_limit} calls/min
  Request Timeout: {config.api.request_timeout}s
  Max Retries: {config.api.max_retries}

Trading Strategy:
  Symbol: {config.trading.default_symbol}
  Ticker: {config.trading.default_ticker}
  Interval: {config.trading.default_interval}
  Period: {config.trading.default_period}
  Features: {', '.join(config.trading.features)}
  Signal Threshold: {config.trading.signal_confidence_threshold}

Model Configuration:
  Type: {config.model.model_type}
  Train/Val/Test Split: {config.model.train_ratio:.1%}/{config.model.val_ratio:.1%}/{config.model.test_ratio:.1%}
  Estimators: {config.model.n_estimators}
  Max Depth: {config.model.max_depth}
  Learning Rate: {config.model.learning_rate}

Data Configuration:
  Raw Data Dir: {config.data.raw_data_dir}
  Processed Data Dir: {config.data.processed_data_dir}
  Max File Age: {config.data.max_file_age_hours}h
  Quality Check Interval: {config.data.data_quality_check_interval}min

Logging:
  Level: {config.logging.level}
  Log Directory: {config.logging.log_dir}
  Log to File: {config.logging.log_to_file}
  Log to Console: {config.logging.log_to_console}
"""

    return report


def main():
    """Main function to run configuration checks."""
    logger.info("Starting F&O AI Trader configuration validation")

    checks = [
        ("Environment Variables", check_environment_variables),
        ("Configuration Structure", check_configuration_structure),
        ("Directory Structure", check_directory_structure),
        ("File Permissions", check_file_permissions),
        ("Python Dependencies", check_python_dependencies)
    ]

    passed_checks = 0
    total_checks = len(checks)

    for check_name, check_function in checks:
        logger.info(f"\n--- {check_name} ---")
        if check_function():
            passed_checks += 1
        else:
            logger.error(f"Check failed: {check_name}")

    # Generate configuration report
    report = generate_configuration_report()
    logger.info(report)

    # Save report to file
    report_file = "logs/configuration_report.txt"
    os.makedirs("logs", exist_ok=True)
    with open(report_file, 'w') as f:
        f.write(report)
    logger.info(f"Configuration report saved to: {report_file}")

    # Final summary
    logger.info(f"\n=== CONFIGURATION CHECK SUMMARY ===")
    logger.info(f"Passed: {passed_checks}/{total_checks} checks")

    if passed_checks == total_checks:
        logger.info("✅ All configuration checks passed!")
        sys.exit(0)
    else:
        logger.error("❌ Some configuration checks failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()