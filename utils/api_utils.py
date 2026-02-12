"""
API utilities for rate limiting and retry logic.
"""
import time
import logging
import functools
from typing import Callable, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter to prevent API abuse."""

    def __init__(self, calls_per_minute: int = 60):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute  # seconds between calls
        self.last_call_time = 0.0

    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time

        if time_since_last_call < self.min_interval:
            wait_time = self.min_interval - time_since_last_call
            logger.info(f"Rate limiting: waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)

        self.last_call_time = time.time()


def retry_with_exponential_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exceptions: Tuple of exceptions to catch and retry on
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries + 1):  # +1 for initial attempt
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries")
                        raise e

                    delay = min(initial_delay * (2 ** attempt), max_delay)
                    logger.warning(f"Function {func.__name__} failed on attempt {attempt + 1}, "
                                   f"retrying in {delay:.2f} seconds: {e}")
                    time.sleep(delay)

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception

        return wrapper
    return decorator


# Global rate limiters for different APIs
NSE_RATE_LIMITER = RateLimiter(calls_per_minute=30)  # Conservative rate limit for NSE
TELEGRAM_RATE_LIMITER = RateLimiter(calls_per_minute=20)  # Telegram bot API limit
YFINANCE_RATE_LIMITER = RateLimiter(calls_per_minute=60)  # YFinance rate limit


def with_rate_limiting(rate_limiter: RateLimiter):
    """Decorator to add rate limiting to API calls."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            rate_limiter.wait_if_needed()
            return func(*args, **kwargs)
        return wrapper
    return decorator