"""
Real-time Signal Monitoring Service
Continuously monitors market conditions and automatically sends CE/PE signals via Telegram.
"""

import time
import signal
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from strategy.advanced_signal_engine import get_signal_engine
from strategy.signal_formatter import get_signal_formatter
from strategy.signal_config import get_signal_config
from execution.alert_system import send_telegram_message, send_alert
from database.db_manager import update_signal_telegram_status
from config.logging_config import get_logger

logger = get_logger('signal_monitor')


class SignalMonitor:
    """Real-time signal monitoring daemon with automatic Telegram delivery."""

    def __init__(self):
        self.config = get_signal_config()
        self.engine = get_signal_engine()
        self.formatter = get_signal_formatter()

        self.running = False
        self.monitor_thread = None
        self.last_heartbeat = datetime.now()
        self.signals_sent = 0
        self.errors_count = 0
        self.cycle_count = 0  # Track monitoring cycles for periodic status logging

        # Signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def start(self, symbol="NIFTY"):
        """Start the signal monitoring service."""
        if self.running:
            logger.warning("Signal monitor is already running")
            return False

        if not self.config.enabled:
            logger.error("Signal generation is disabled in configuration")
            return False

        logger.info("Starting signal monitoring service...")

        self.running = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(symbol,),
            daemon=False,
            name="SignalMonitor"
        )
        self.monitor_thread.start()

        # Send startup notification
        startup_message = self.formatter.format_status_message(
            "STARTED",
            f"Monitoring {symbol} every {self.config.monitoring_interval_seconds}s"
        )
        self._send_notification(startup_message)

        logger.info(f"Signal monitor started for {symbol}")
        return True

    def stop(self):
        """Stop the signal monitoring service."""
        if not self.running:
            logger.warning("Signal monitor is not running")
            return

        logger.info("Stopping signal monitoring service...")

        self.running = False

        # Wait for monitor thread to finish
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=10)

        # Send shutdown notification
        shutdown_message = self.formatter.format_status_message(
            "STOPPED",
            f"Sent {self.signals_sent} signals, {self.errors_count} errors"
        )
        self._send_notification(shutdown_message)

        logger.info("Signal monitor stopped")

    def get_status(self) -> Dict:
        """Get current status of the signal monitor."""
        engine_status = self.engine.get_signal_status()
        return {
            'running': self.running,
            'last_heartbeat': self.last_heartbeat,
            'signals_sent': self.signals_sent,
            'errors_count': self.errors_count,
            'cycle_count': self.cycle_count,
            'market_hours_enforcement': self.config.market_hours_only,
            'signal_renewal_enabled': engine_status.get('signal_renewal_enabled', False),
            'active_signals_count': engine_status.get('active_signals_count', 0),
            'config': self.config.get_config_summary(),
            'engine_status': engine_status
        }

    def _monitor_loop(self, symbol):
        """Main monitoring loop that runs in a separate thread."""
        logger.info(f"Signal monitoring loop started for {symbol}")

        while self.running:
            try:
                cycle_start = time.time()
                self.last_heartbeat = datetime.now()
                self.cycle_count += 1

                # Skip if outside market hours
                if self.config.market_hours_only and not self.config.is_market_hours():
                    logger.debug("Outside market hours, waiting...")
                    self._safe_sleep(self.config.monitoring_interval_seconds)
                    continue

                # Periodic detailed status logging (every 10 cycles during market hours)
                if self.cycle_count % 10 == 0:
                    engine_status = self.engine.get_signal_status()
                    active_count = engine_status.get('active_signals_count', 0)
                    renewal_enabled = engine_status.get('signal_renewal_enabled', False)
                    logger.info(f"Monitor status: cycle={self.cycle_count}, active_signals={active_count}, "
                              f"renewal_enabled={renewal_enabled}, signals_sent={self.signals_sent}")

                # Generate and process signals
                signals = self._generate_and_process_signals(symbol)

                if signals:
                    logger.info(f"Processing {len(signals)} new signals")
                    self._process_signals(signals)
                else:
                    logger.debug("No new signals generated")

                # Calculate sleep time to maintain consistent interval
                cycle_time = time.time() - cycle_start
                sleep_time = max(0, self.config.monitoring_interval_seconds - cycle_time)

                if sleep_time > 0:
                    self._safe_sleep(sleep_time)
                else:
                    logger.warning(f"Signal processing took {cycle_time:.1f}s, "
                                 f"longer than interval {self.config.monitoring_interval_seconds}s")

            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, stopping monitor...")
                break
            except Exception as e:
                self.errors_count += 1
                logger.error(f"Error in monitoring loop: {e}", exc_info=True)

                # Send error notification if too many errors
                if self.errors_count % 5 == 0:  # Every 5 errors
                    error_message = self.formatter.format_error_message(
                        f"Monitor errors: {self.errors_count}"
                    )
                    self._send_notification(error_message)

                # Sleep longer after error to avoid tight error loops
                self._safe_sleep(min(60, self.config.monitoring_interval_seconds * 2))

        logger.info("Signal monitoring loop ended")

    def _generate_and_process_signals(self, symbol) -> List[Dict]:
        """Generate signals using the advanced engine."""
        try:
            # Log current active signals status (if any)
            active_signals = self.engine.get_active_signals()
            if active_signals:
                active_types = list(active_signals.keys())
                logger.debug(f"Active signals before generation: {active_types}")

            # Generate new signals (engine handles expiry cleanup internally)
            signals = self.engine.generate_signals(symbol)

            # Log signal generation results
            if signals:
                signal_types = [s['signal_type'] for s in signals]
                logger.info(f"Generated new signals: {signal_types}")
            else:
                logger.debug("No new signals generated")

            return signals if signals else []

        except Exception as e:
            logger.error(f"Error generating signals: {e}")
            return []

    def _process_signals(self, signals: List[Dict]):
        """Process and send signals via Telegram."""
        for signal in signals:
            try:
                self._send_signal(signal)
            except Exception as e:
                logger.error(f"Error processing signal {signal.get('signal_type', 'UNKNOWN')}: {e}")

    def _send_signal(self, signal: Dict):
        """Send individual signal via Telegram and update database."""
        try:
            # Format the signal message
            message = self.formatter.format_signal_message(signal)

            # Send via Telegram
            success = send_telegram_message(message)

            if success:
                self.signals_sent += 1
                logger.info(f"Signal sent successfully: {signal['signal_type']} at {signal['strike_price']}")

                # Update database with successful delivery
                if 'id' in signal:  # If signal has database ID
                    update_signal_telegram_status(signal['id'], sent_successfully=True)

            else:
                logger.error(f"Failed to send signal: {signal['signal_type']} at {signal['strike_price']}")

                # Update database with failed delivery
                if 'id' in signal:
                    update_signal_telegram_status(signal['id'], sent_successfully=False)

        except Exception as e:
            logger.error(f"Error sending signal: {e}")

    def _send_notification(self, message: str):
        """Send system notification (startup, shutdown, errors)."""
        try:
            send_telegram_message(message)
            logger.debug("System notification sent")
        except Exception as e:
            logger.error(f"Failed to send system notification: {e}")

    def _safe_sleep(self, seconds: float):
        """Sleep while checking for shutdown signal."""
        sleep_intervals = max(1, int(seconds))
        for _ in range(sleep_intervals):
            if not self.running:
                break
            time.sleep(1)

        # Handle fractional remainder
        remaining = seconds - sleep_intervals
        if remaining > 0 and self.running:
            time.sleep(remaining)

    def _signal_handler(self, signum, frame):
        """Handle system signals for graceful shutdown."""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.stop()

    def send_test_signal(self, symbol="NIFTY"):
        """Send a test signal for testing purposes."""
        test_signal = {
            'signal_type': 'BUY_CE',
            'strike_price': 21000,
            'signal_strength': 'HIGH',
            'confidence_score': 0.85,
            'pcr_value': 0.65,
            'rsi_value': 42,
            'oi_change_pct': 18.5,
            'spot_price': 20980,
            'premium_price': 150,
            'target_price': 200,
            'stop_loss': 120,
            'generated_at': datetime.now(),
            'symbol': symbol,
            'validity_minutes': 15,
            'market_context': 'TEST SIGNAL - Strong breakout pattern'
        }

        message = self.formatter.format_signal_message(test_signal)
        success = send_telegram_message(f"🧪 TEST SIGNAL\n\n{message}")

        if success:
            logger.info("Test signal sent successfully")
            return True
        else:
            logger.error("Failed to send test signal")
            return False


# Global monitor instance
_signal_monitor = None


def get_signal_monitor():
    """Get global signal monitor instance."""
    global _signal_monitor
    if _signal_monitor is None:
        _signal_monitor = SignalMonitor()
    return _signal_monitor


# CLI functions for easy use
def start_monitoring(symbol="NIFTY"):
    """Start signal monitoring service."""
    monitor = get_signal_monitor()
    return monitor.start(symbol)


def stop_monitoring():
    """Stop signal monitoring service."""
    monitor = get_signal_monitor()
    monitor.stop()


def get_monitoring_status():
    """Get monitoring service status."""
    monitor = get_signal_monitor()
    return monitor.get_status()


def send_test_signal(symbol="NIFTY"):
    """Send test signal for verification."""
    monitor = get_signal_monitor()
    return monitor.send_test_signal(symbol)


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="F&O AI Trader Signal Monitor")
    parser.add_argument('--symbol', default='NIFTY', help='Symbol to monitor (default: NIFTY)')
    parser.add_argument('--test', action='store_true', help='Send test signal and exit')
    parser.add_argument('--status', action='store_true', help='Show status and exit')
    parser.add_argument('--daemon', action='store_true', help='Run as daemon (default behavior)')

    args = parser.parse_args()

    if args.test:
        print("Sending test signal...")
        success = send_test_signal(args.symbol)
        print(f"Test signal {'sent successfully' if success else 'failed'}")
        exit(0)

    if args.status:
        status = get_monitoring_status()
        print("Signal Monitor Status:")
        print(json.dumps(status, indent=2, default=str))
        exit(0)

    # Default: Start monitoring service
    try:
        logger.info(f"Starting signal monitor for {args.symbol}...")
        print(f"🤖 F&O AI Trader Signal Monitor")
        print(f"📊 Monitoring: {args.symbol}")
        print(f"⏰ Interval: {get_signal_config().monitoring_interval_seconds}s")
        print(f"🕐 Market Hours: {get_signal_config().market_start_time} - {get_signal_config().market_end_time}")
        print("Press Ctrl+C to stop...")

        success = start_monitoring(args.symbol)

        if success:
            # Keep main thread alive
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n🛑 Shutting down signal monitor...")
                stop_monitoring()
        else:
            print("❌ Failed to start signal monitoring")
            exit(1)

    except Exception as e:
        logger.error(f"Error running signal monitor: {e}", exc_info=True)
        print(f"❌ Error: {e}")
        exit(1)