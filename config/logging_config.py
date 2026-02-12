"""
Centralized logging configuration for F&O AI Trader.
"""
import logging
import logging.handlers
import os
from datetime import datetime


def setup_logging(
    log_level: str = "INFO",
    log_to_file: bool = True,
    log_to_console: bool = True,
    log_dir: str = "logs",
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
):
    """
    Set up centralized logging configuration.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to log to file
        log_to_console: Whether to log to console
        log_dir: Directory for log files
        max_file_size: Maximum size of log file before rotation
        backup_count: Number of backup log files to keep
    """
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Clear any existing handlers
    root_logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        root_logger.addHandler(console_handler)

    # File handler with rotation
    if log_to_file:
        log_filename = os.path.join(log_dir, f"fo_trader_{datetime.now().strftime('%Y%m%d')}.log")
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_filename,
            maxBytes=max_file_size,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        root_logger.addHandler(file_handler)

    # Create separate loggers for different components
    _setup_component_loggers()

    logging.info("Logging system initialized")


def _setup_component_loggers():
    """Set up specialized loggers for different components."""

    # Trading signals logger
    signals_logger = logging.getLogger('signals')
    signals_handler = logging.handlers.RotatingFileHandler(
        filename='logs/trading_signals.log',
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=10
    )
    signals_formatter = logging.Formatter(
        fmt='%(asctime)s - SIGNAL - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    signals_handler.setFormatter(signals_formatter)
    signals_logger.addHandler(signals_handler)
    signals_logger.setLevel(logging.INFO)

    # API calls logger
    api_logger = logging.getLogger('api')
    api_handler = logging.handlers.RotatingFileHandler(
        filename='logs/api_calls.log',
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    api_formatter = logging.Formatter(
        fmt='%(asctime)s - API - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    api_handler.setFormatter(api_formatter)
    api_logger.addHandler(api_handler)
    api_logger.setLevel(logging.DEBUG)

    # Database operations logger
    db_logger = logging.getLogger('database')
    db_handler = logging.handlers.RotatingFileHandler(
        filename='logs/database.log',
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=3
    )
    db_formatter = logging.Formatter(
        fmt='%(asctime)s - DB - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    db_handler.setFormatter(db_formatter)
    db_logger.addHandler(db_handler)
    db_logger.setLevel(logging.INFO)

    # Errors logger for critical issues
    error_logger = logging.getLogger('errors')
    error_handler = logging.handlers.RotatingFileHandler(
        filename='logs/errors.log',
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=10
    )
    error_formatter = logging.Formatter(
        fmt='%(asctime)s - ERROR - %(name)s - %(levelname)s - %(message)s - %(pathname)s:%(lineno)d',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    error_handler.setFormatter(error_formatter)
    error_logger.addHandler(error_handler)
    error_logger.setLevel(logging.ERROR)


def get_logger(name: str = None) -> logging.Logger:
    """
    Get a logger instance with the specified name.

    Args:
        name: Logger name. If None, returns root logger.

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def log_trade_signal(signal: str, confidence: float = None, metadata: dict = None):
    """
    Log trading signals to the specialized signals logger.

    Args:
        signal: Trading signal description
        confidence: Confidence score (0-1) if available
        metadata: Additional metadata dictionary
    """
    signals_logger = logging.getLogger('signals')

    log_message = f"SIGNAL: {signal}"
    if confidence is not None:
        log_message += f" | Confidence: {confidence:.2f}"
    if metadata:
        log_message += f" | Metadata: {metadata}"

    signals_logger.info(log_message)


def log_api_call(api_name: str, endpoint: str, status: str, response_time: float = None, error: str = None):
    """
    Log API calls to the specialized API logger.

    Args:
        api_name: Name of the API (e.g., 'NSE', 'YFinance', 'Telegram')
        endpoint: API endpoint or description
        status: Call status ('SUCCESS', 'FAILED', 'RETRY')
        response_time: Response time in seconds
        error: Error message if failed
    """
    api_logger = logging.getLogger('api')

    log_message = f"API_CALL: {api_name} | {endpoint} | {status}"
    if response_time is not None:
        log_message += f" | Response time: {response_time:.2f}s"
    if error:
        log_message += f" | Error: {error}"

    if status == 'SUCCESS':
        api_logger.info(log_message)
    else:
        api_logger.warning(log_message)


# Environment-based configuration
def setup_production_logging():
    """Set up logging for production environment."""
    setup_logging(
        log_level="INFO",
        log_to_file=True,
        log_to_console=False,  # Reduce console output in production
        max_file_size=50 * 1024 * 1024,  # 50MB
        backup_count=10
    )


def setup_development_logging():
    """Set up logging for development environment."""
    setup_logging(
        log_level="DEBUG",
        log_to_file=True,
        log_to_console=True,
        max_file_size=10 * 1024 * 1024,  # 10MB
        backup_count=3
    )


# Initialize logging on import
if os.getenv('TRADING_ENV') == 'production':
    setup_production_logging()
else:
    setup_development_logging()