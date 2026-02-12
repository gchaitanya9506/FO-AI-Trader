"""
Signal Configuration Management System
Loads and manages signal parameters from settings.yaml with validation and defaults.
"""

from config.app_config import get_config
from config.logging_config import get_logger
from datetime import datetime, time

logger = get_logger('signal_config')

class SignalConfig:
    """Manages signal generation configuration with validation and defaults."""

    def __init__(self):
        self.config = get_config()
        self._load_signal_config()

    def _load_signal_config(self):
        """Load signal configuration from settings.yaml with validation."""
        try:
            # Get signal_generation section with defaults
            signal_config = getattr(self.config, 'signal_generation', {})

            # Main settings
            self.enabled = signal_config.get('enabled', True)
            self.monitoring_interval_seconds = signal_config.get('monitoring_interval_seconds', 120)
            self.market_hours_only = signal_config.get('market_hours_only', True)

            # PCR thresholds
            pcr_config = signal_config.get('pcr_thresholds', {})
            self.pcr_buy_ce_max = pcr_config.get('buy_ce_max', 0.7)
            self.pcr_buy_pe_min = pcr_config.get('buy_pe_min', 1.3)
            self.pcr_neutral_range = pcr_config.get('neutral_range', [0.8, 1.2])

            # RSI levels
            rsi_config = signal_config.get('rsi_levels', {})
            self.rsi_oversold_max = rsi_config.get('oversold_max', 30)
            self.rsi_oversold_recovery = rsi_config.get('oversold_recovery', 50)
            self.rsi_overbought_min = rsi_config.get('overbought_min', 70)
            self.rsi_overbought_decline = rsi_config.get('overbought_decline', 50)

            # OI analysis
            oi_config = signal_config.get('oi_analysis', {})
            self.oi_significant_change_pct = oi_config.get('significant_change_pct', 15)
            self.oi_volume_spike_multiplier = oi_config.get('volume_spike_multiplier', 2)
            self.oi_min_level = oi_config.get('min_oi_level', 10000)

            # Signal management
            self.signal_cooldown_minutes = signal_config.get('signal_cooldown_minutes', 15)
            self.confidence_threshold = signal_config.get('confidence_threshold', 0.7)
            self.max_signals_per_hour = signal_config.get('max_signals_per_hour', 6)

            # Signal expiry and renewal (NEW)
            self.enable_signal_renewal = signal_config.get('enable_signal_renewal', True)
            self.signal_expiry_buffer_seconds = signal_config.get('signal_expiry_buffer_seconds', 30)

            # Market hours from data_pipeline section
            data_pipeline = getattr(self.config, 'data_pipeline', {})
            market_hours = data_pipeline.get('market_hours', {})
            self.market_start_time = market_hours.get('start_time', '09:15')
            self.market_end_time = market_hours.get('end_time', '15:30')
            self.timezone = market_hours.get('timezone', 'Asia/Kolkata')

            self._validate_config()
            logger.info("Signal configuration loaded successfully")

        except Exception as e:
            logger.error(f"Error loading signal configuration: {e}")
            self._set_defaults()

    def _validate_config(self):
        """Validate configuration parameters."""
        # Validate PCR thresholds
        if self.pcr_buy_ce_max >= self.pcr_buy_pe_min:
            raise ValueError(f"PCR buy_ce_max ({self.pcr_buy_ce_max}) must be less than buy_pe_min ({self.pcr_buy_pe_min})")

        # Validate RSI levels
        if not (0 <= self.rsi_oversold_max < self.rsi_oversold_recovery <= 100):
            raise ValueError(f"Invalid RSI oversold configuration: max={self.rsi_oversold_max}, recovery={self.rsi_oversold_recovery}")

        if not (0 <= self.rsi_overbought_decline < self.rsi_overbought_min <= 100):
            raise ValueError(f"Invalid RSI overbought configuration: decline={self.rsi_overbought_decline}, min={self.rsi_overbought_min}")

        # Validate confidence threshold
        if not (0 < self.confidence_threshold <= 1):
            raise ValueError(f"Confidence threshold must be between 0 and 1, got {self.confidence_threshold}")

        # Validate cooldown and rate limiting
        if self.signal_cooldown_minutes < 0:
            raise ValueError(f"Signal cooldown cannot be negative: {self.signal_cooldown_minutes}")

        if self.max_signals_per_hour <= 0:
            raise ValueError(f"Max signals per hour must be positive: {self.max_signals_per_hour}")

    def _set_defaults(self):
        """Set default configuration when loading fails."""
        logger.warning("Using default signal configuration due to loading error")

        # Main settings
        self.enabled = True
        self.monitoring_interval_seconds = 120
        self.market_hours_only = True

        # PCR thresholds
        self.pcr_buy_ce_max = 0.7
        self.pcr_buy_pe_min = 1.3
        self.pcr_neutral_range = [0.8, 1.2]

        # RSI levels
        self.rsi_oversold_max = 30
        self.rsi_oversold_recovery = 50
        self.rsi_overbought_min = 70
        self.rsi_overbought_decline = 50

        # OI analysis
        self.oi_significant_change_pct = 15
        self.oi_volume_spike_multiplier = 2
        self.oi_min_level = 10000

        # Signal management
        self.signal_cooldown_minutes = 15
        self.confidence_threshold = 0.7
        self.max_signals_per_hour = 6

        # Signal expiry and renewal
        self.enable_signal_renewal = True
        self.signal_expiry_buffer_seconds = 30

        # Market hours
        self.market_start_time = '09:15'
        self.market_end_time = '15:30'
        self.timezone = 'Asia/Kolkata'

    def is_market_hours(self, current_time=None):
        """Check if current time is within market hours."""
        if not self.market_hours_only:
            return True

        if current_time is None:
            current_time = datetime.now()

        try:
            # Parse market times
            start_time = datetime.strptime(self.market_start_time, '%H:%M').time()
            end_time = datetime.strptime(self.market_end_time, '%H:%M').time()
            current_time_only = current_time.time()

            # Check if current time is within market hours
            return start_time <= current_time_only <= end_time

        except Exception as e:
            logger.error(f"Error checking market hours: {e}")
            return True  # Default to allowing signals if time check fails

    def is_pcr_bullish(self, pcr_value):
        """Check if PCR indicates bullish conditions (BUY CE)."""
        return pcr_value <= self.pcr_buy_ce_max

    def is_pcr_bearish(self, pcr_value):
        """Check if PCR indicates bearish conditions (BUY PE)."""
        return pcr_value >= self.pcr_buy_pe_min

    def is_pcr_neutral(self, pcr_value):
        """Check if PCR is in neutral range."""
        return self.pcr_neutral_range[0] <= pcr_value <= self.pcr_neutral_range[1]

    def is_rsi_oversold_recovery(self, current_rsi, previous_rsi):
        """Check if RSI is recovering from oversold."""
        return (previous_rsi <= self.rsi_oversold_max and
                current_rsi > self.rsi_oversold_max and
                current_rsi <= self.rsi_oversold_recovery)

    def is_rsi_overbought_decline(self, current_rsi, previous_rsi):
        """Check if RSI is declining from overbought."""
        return (previous_rsi >= self.rsi_overbought_min and
                current_rsi < self.rsi_overbought_min and
                current_rsi >= self.rsi_overbought_decline)

    def is_oi_change_significant(self, oi_change_pct):
        """Check if OI change is significant."""
        return abs(oi_change_pct) >= self.oi_significant_change_pct

    def is_oi_level_sufficient(self, oi_level):
        """Check if OI level is above minimum threshold."""
        return oi_level >= self.oi_min_level

    def get_config_summary(self):
        """Get a summary of current configuration."""
        return {
            'enabled': self.enabled,
            'monitoring_interval_seconds': self.monitoring_interval_seconds,
            'market_hours_only': self.market_hours_only,
            'pcr_thresholds': {
                'buy_ce_max': self.pcr_buy_ce_max,
                'buy_pe_min': self.pcr_buy_pe_min,
                'neutral_range': self.pcr_neutral_range
            },
            'rsi_levels': {
                'oversold_max': self.rsi_oversold_max,
                'oversold_recovery': self.rsi_oversold_recovery,
                'overbought_min': self.rsi_overbought_min,
                'overbought_decline': self.rsi_overbought_decline
            },
            'oi_analysis': {
                'significant_change_pct': self.oi_significant_change_pct,
                'volume_spike_multiplier': self.oi_volume_spike_multiplier,
                'min_oi_level': self.oi_min_level
            },
            'signal_management': {
                'cooldown_minutes': self.signal_cooldown_minutes,
                'confidence_threshold': self.confidence_threshold,
                'max_signals_per_hour': self.max_signals_per_hour
            },
            'signal_renewal': {
                'enable_signal_renewal': self.enable_signal_renewal,
                'signal_expiry_buffer_seconds': self.signal_expiry_buffer_seconds
            },
            'market_hours': {
                'start_time': self.market_start_time,
                'end_time': self.market_end_time,
                'timezone': self.timezone
            }
        }

# Global instance
_signal_config = None

def get_signal_config():
    """Get global signal configuration instance."""
    global _signal_config
    if _signal_config is None:
        _signal_config = SignalConfig()
    return _signal_config

if __name__ == "__main__":
    # Test configuration loading
    config = get_signal_config()
    print("Signal Configuration Summary:")
    import json
    print(json.dumps(config.get_config_summary(), indent=2))