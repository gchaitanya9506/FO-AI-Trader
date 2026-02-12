# automated_scheduler.py
"""
Intelligent Automated Scheduler for NSE Option Chain Data
Replaces the broken scheduler.py with market-aware scheduling, error handling, and retry logic.
"""

import time
import schedule
from datetime import datetime, timedelta
import pytz
from typing import Optional, Dict, Any
import threading
import signal
import sys

from config.logging_config import get_logger
from config.app_config import get_config
from data_processor import OptionChainProcessor
from fetch_data import fetch_nifty_data


class AutomatedScheduler:
    """
    Intelligent scheduler that fetches NSE data during market hours with proper error handling.
    """

    def __init__(self):
        self.config = get_config()
        self.logger = get_logger('scheduler')
        self.processor = OptionChainProcessor()
        self.is_running = False
        self.market_timezone = pytz.timezone(self.config.data_pipeline.market_hours.timezone)
        self.consecutive_failures = 0
        self.max_consecutive_failures = 10
        self.last_successful_fetch = None

        # Set up graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
        sys.exit(0)

    def is_market_open(self) -> bool:
        """
        Check if NSE market is currently open.
        Market hours: 9:15 AM - 3:30 PM IST, Monday to Friday
        """
        now = datetime.now(self.market_timezone)

        # Check if it's a weekday (Monday=0, Sunday=6)
        if now.weekday() > 4:  # Saturday or Sunday
            return False

        # Parse market hours from config
        start_time_str = self.config.data_pipeline.market_hours.start_time  # "09:15"
        end_time_str = self.config.data_pipeline.market_hours.end_time      # "15:30"

        start_hour, start_min = map(int, start_time_str.split(':'))
        end_hour, end_min = map(int, end_time_str.split(':'))

        market_start = now.replace(hour=start_hour, minute=start_min, second=0, microsecond=0)
        market_end = now.replace(hour=end_hour, minute=end_min, second=0, microsecond=0)

        return market_start <= now <= market_end

    def should_fetch_data(self) -> bool:
        """
        Determine if data should be fetched now based on market hours and configuration.
        """
        # Always fetch if market is open
        if self.is_market_open():
            return True

        # Check pre-market and post-market settings
        now = datetime.now(self.market_timezone)
        start_time_str = self.config.data_pipeline.market_hours.start_time
        end_time_str = self.config.data_pipeline.market_hours.end_time

        start_hour, start_min = map(int, start_time_str.split(':'))
        end_hour, end_min = map(int, end_time_str.split(':'))

        market_start = now.replace(hour=start_hour, minute=start_min, second=0, microsecond=0)
        market_end = now.replace(hour=end_hour, minute=end_min, second=0, microsecond=0)

        # Pre-market: 1 hour before market opens
        if self.config.data_pipeline.pre_market_fetch:
            pre_market_start = market_start - timedelta(hours=1)
            if pre_market_start <= now < market_start:
                self.logger.info("Pre-market data fetch enabled")
                return True

        # Post-market: 2 hours after market closes
        if self.config.data_pipeline.post_market_fetch:
            post_market_end = market_end + timedelta(hours=2)
            if market_end < now <= post_market_end:
                self.logger.info("Post-market data fetch enabled")
                return True

        return False

    def fetch_nifty_data_with_retry(self) -> bool:
        """Fetch NIFTY underlying data with retry logic."""
        max_retries = self.config.data_pipeline.retry_policy.max_retries
        backoff = self.config.data_pipeline.retry_policy.backoff_multiplier
        initial_delay = self.config.data_pipeline.retry_policy.initial_delay

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Fetching NIFTY data (attempt {attempt + 1}/{max_retries})")

                # Fetch both 5m intervals for different purposes
                nifty_result = fetch_nifty_data(
                    interval="5m",
                    ticker="^NSEI",
                    period="30d",
                    save_path="data/raw/nifty_data_5m.csv"
                )

                if nifty_result is not None:
                    self.logger.info("Successfully fetched NIFTY data")
                    return True
                else:
                    self.logger.warning(f"NIFTY data fetch returned None on attempt {attempt + 1}")

            except Exception as e:
                self.logger.error(f"Error fetching NIFTY data on attempt {attempt + 1}: {e}")

            # Wait before retry (except on last attempt)
            if attempt < max_retries - 1:
                wait_time = initial_delay * (backoff ** attempt)
                self.logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

        self.logger.error("Failed to fetch NIFTY data after all retry attempts")
        return False

    def fetch_option_chain_with_retry(self) -> bool:
        """Fetch option chain data with retry logic using automated processor."""
        max_retries = self.config.data_pipeline.retry_policy.max_retries

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Fetching option chain data (attempt {attempt + 1}/{max_retries})")

                result = self.processor.fetch_and_process_option_chain(
                    symbol="NIFTY",
                    save_to_database=self.config.data_pipeline.enable_database_direct,
                    save_to_csv=True  # Keep CSV for backward compatibility
                )

                if result is not None and not result.empty:
                    self.logger.info(f"Successfully processed {len(result)} option chain records")
                    return True
                else:
                    self.logger.warning(f"Option chain processing returned empty result on attempt {attempt + 1}")

            except Exception as e:
                self.logger.error(f"Error processing option chain on attempt {attempt + 1}: {e}")

            # Processor has its own retry logic, so don't add extra delay here
            if attempt < max_retries - 1:
                self.logger.info("Retrying option chain fetch...")

        self.logger.error("Failed to process option chain data after all retry attempts")
        return False

    def fetch_all_data(self) -> Dict[str, bool]:
        """
        Fetch all required market data.
        Returns dict with success status for each data type.
        """
        results = {
            'nifty': False,
            'option_chain': False
        }

        # Check if we should fetch data now
        if not self.should_fetch_data():
            self.logger.debug("Skipping data fetch - outside market hours and fetch windows")
            return results

        self.logger.info("Starting scheduled data fetch...")

        # Fetch NIFTY underlying data
        try:
            results['nifty'] = self.fetch_nifty_data_with_retry()
        except Exception as e:
            self.logger.error(f"Unexpected error fetching NIFTY data: {e}")

        # Fetch option chain data
        try:
            results['option_chain'] = self.fetch_option_chain_with_retry()
        except Exception as e:
            self.logger.error(f"Unexpected error fetching option chain: {e}")

        # Update failure tracking
        if results['nifty'] and results['option_chain']:
            self.consecutive_failures = 0
            self.last_successful_fetch = datetime.now(self.market_timezone)
            self.logger.info("✅ All data fetched successfully")
        else:
            self.consecutive_failures += 1
            self.logger.warning(f"⚠️ Partial or failed data fetch (failures: {self.consecutive_failures})")

            # Alert on too many consecutive failures
            if self.consecutive_failures >= self.max_consecutive_failures:
                self.logger.error(f"🚨 {self.consecutive_failures} consecutive failures - system may need attention")

        return results

    def cleanup_old_data(self):
        """Clean up old data based on retention policy."""
        try:
            from database.db_manager import cleanup_old_option_data

            retention_days = self.config.data_pipeline.data_retention_days
            deleted_count = cleanup_old_option_data(retention_days)

            if deleted_count > 0:
                self.logger.info(f"Cleaned up {deleted_count} old records (retention: {retention_days} days)")

        except Exception as e:
            self.logger.error(f"Error during data cleanup: {e}")

    def setup_schedule(self):
        """Setup the scheduled tasks based on configuration."""
        interval_seconds = self.config.data_pipeline.fetch_interval_seconds

        # Schedule data fetching
        schedule.every(interval_seconds).seconds.do(self.fetch_all_data)

        # Schedule daily cleanup at midnight
        schedule.every().day.at("00:00").do(self.cleanup_old_data)

        self.logger.info(f"Scheduler configured:")
        self.logger.info(f"  - Data fetch interval: {interval_seconds} seconds")
        self.logger.info(f"  - Market hours: {self.config.data_pipeline.market_hours.start_time} - {self.config.data_pipeline.market_hours.end_time} {self.config.data_pipeline.market_hours.timezone}")
        self.logger.info(f"  - Pre-market fetch: {self.config.data_pipeline.pre_market_fetch}")
        self.logger.info(f"  - Post-market fetch: {self.config.data_pipeline.post_market_fetch}")
        self.logger.info(f"  - Database direct: {self.config.data_pipeline.enable_database_direct}")

    def run(self):
        """Start the automated scheduler."""
        self.logger.info("🚀 Starting Automated NSE Option Chain Scheduler")

        # Setup schedule
        self.setup_schedule()

        # Run initial data fetch
        self.logger.info("Performing initial data fetch...")
        self.fetch_all_data()

        self.is_running = True

        # Main scheduler loop
        self.logger.info("📅 Scheduler running - press Ctrl+C to stop")

        try:
            while self.is_running:
                schedule.run_pending()
                time.sleep(1)  # Check every second

        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            self.stop()

    def stop(self):
        """Stop the scheduler gracefully."""
        if self.is_running:
            self.logger.info("🛑 Stopping automated scheduler...")
            self.is_running = False
            schedule.clear()
        else:
            self.logger.debug("Scheduler is already stopped")

    def get_status(self) -> Dict[str, Any]:
        """Get current scheduler status."""
        return {
            'is_running': self.is_running,
            'market_open': self.is_market_open(),
            'should_fetch': self.should_fetch_data(),
            'consecutive_failures': self.consecutive_failures,
            'last_successful_fetch': self.last_successful_fetch,
            'next_run': schedule.next_run(),
            'scheduled_jobs': len(schedule.jobs)
        }


def main():
    """Main function to run the automated scheduler."""
    try:
        scheduler = AutomatedScheduler()
        scheduler.run()
    except Exception as e:
        logger = get_logger('scheduler')
        logger.error(f"Fatal error in automated scheduler: {e}")
        raise


if __name__ == "__main__":
    main()