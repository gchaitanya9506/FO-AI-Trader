"""
Advanced Signal Engine with Multi-Indicator Logic
Combines PCR, Open Interest analysis, and RSI to generate precise BUY CE/BUY PE signals.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from database.db_manager import load_table, get_latest_option_chain_data, save_trading_signal
from feature_engineering.oi_signals import compute_window_pcr, get_atm_strike
from strategy.signal_config import get_signal_config
from strategy.signal_formatter import get_signal_formatter
from config.logging_config import get_logger

logger = get_logger('advanced_signal_engine')


class AdvancedSignalEngine:
    """Advanced signal engine combining PCR, OI analysis, and RSI indicators."""

    def __init__(self):
        self.config = get_signal_config()
        self.formatter = get_signal_formatter()
        self.last_signals = {}  # Track last signal times for cooldown
        self.signal_history = []  # Track recent signals for rate limiting
        self.active_signals = {}  # Track active signals with expiry times: {'BUY_CE': signal_data, 'BUY_PE': signal_data}

    def generate_signals(self, symbol="NIFTY"):
        """
        Generate trading signals using multi-indicator analysis.

        Args:
            symbol: Trading symbol to analyze

        Returns:
            List of signal dictionaries or empty list if no signals
        """
        try:
            if not self.config.enabled:
                logger.debug("Signal generation is disabled in configuration")
                return []

            if not self.config.is_market_hours():
                logger.debug("Outside market hours, skipping signal generation")
                return []

            # Clear any expired signals before generating new ones
            self.clear_expired_signals()

            # Check rate limiting
            if not self._check_rate_limits():
                logger.debug("Rate limit exceeded, skipping signal generation")
                return []

            # Get latest market data
            market_data = self._get_market_data(symbol)
            if not market_data:
                logger.warning("Failed to get market data for signal generation")
                return []

            # Generate signals using multi-indicator logic
            signals = self._analyze_market_conditions(market_data, symbol)

            # Filter signals by confidence threshold
            valid_signals = [
                signal for signal in signals
                if signal.get('confidence_score', 0) >= self.config.confidence_threshold
            ]

            # Save signals to database and track for rate limiting and expiry
            for signal in valid_signals:
                self._save_signal_to_database(signal)
                self._track_signal_for_rate_limiting(signal)
                # Track active signals for expiry-based renewal
                self.track_active_signal(signal)

            if valid_signals:
                logger.info(f"Generated {len(valid_signals)} valid signals for {symbol}")
            else:
                logger.debug(f"No valid signals generated for {symbol}")

            return valid_signals

        except Exception as e:
            logger.error(f"Error generating signals: {e}", exc_info=True)
            return []

    def _get_market_data(self, symbol):
        """Retrieve and validate market data for signal generation."""
        try:
            # Get latest underlying features (RSI, price data)
            underlying_df = load_table("underlying_features")
            if underlying_df.empty:
                logger.error("No underlying features data available")
                return None

            # Get latest option chain data
            option_chain_df = get_latest_option_chain_data(symbol, limit=1000)
            if option_chain_df.empty:
                logger.error("No option chain data available")
                return None

            # Get latest records
            latest_underlying = underlying_df.tail(2)  # Get last 2 for RSI comparison
            latest_spot_price = latest_underlying['Close'].iloc[-1] if not latest_underlying.empty else None

            if latest_spot_price is None:
                logger.error("No spot price available")
                return None

            # Filter option chain for recent data (within last hour)
            recent_cutoff = pd.Timestamp.now() - pd.Timedelta(hours=1)
            recent_option_chain = option_chain_df[option_chain_df['date'] >= recent_cutoff]

            if recent_option_chain.empty:
                logger.warning("No recent option chain data available")
                # Use latest available data with warning
                recent_option_chain = option_chain_df.head(100)

            market_data = {
                'underlying_data': latest_underlying,
                'option_chain_data': recent_option_chain,
                'spot_price': latest_spot_price,
                'timestamp': pd.Timestamp.now()
            }

            logger.debug(f"Retrieved market data: spot={latest_spot_price:.2f}, "
                        f"underlying_records={len(latest_underlying)}, "
                        f"option_records={len(recent_option_chain)}")

            return market_data

        except Exception as e:
            logger.error(f"Error retrieving market data: {e}")
            return None

    def _analyze_market_conditions(self, market_data, symbol):
        """Analyze market conditions and generate signals."""
        signals = []

        try:
            underlying_data = market_data['underlying_data']
            option_chain_data = market_data['option_chain_data']
            spot_price = market_data['spot_price']

            # Map database column names to expected format for PCR calculation
            option_chain_mapped = option_chain_data.rename(columns={
                'strike_price': 'Strike Price',
                'option_type': 'Option Type',
                'open_interest': 'Open Interest',
                'change_in_oi': 'Change in OI'
            })

            # Calculate PCR using window approach
            pcr_value, atm_strike, window_range = compute_window_pcr(
                option_chain_mapped,
                spot_price,
                step=50,
                window=2
            )

            if pcr_value == 0:
                logger.warning("PCR calculation returned 0, skipping signal generation")
                return signals

            # Get RSI values (current and previous)
            current_rsi = underlying_data['rsi'].iloc[-1] if len(underlying_data) >= 1 else None
            previous_rsi = underlying_data['rsi'].iloc[-2] if len(underlying_data) >= 2 else current_rsi

            if current_rsi is None:
                logger.warning("RSI data not available, skipping signal generation")
                return signals

            # Analyze OI patterns around ATM
            oi_analysis = self._analyze_oi_patterns(option_chain_data, spot_price, atm_strike)

            # Generate CE signals (bullish)
            ce_signal = self._evaluate_ce_conditions(
                pcr_value, current_rsi, previous_rsi, oi_analysis, spot_price, atm_strike, symbol
            )
            if ce_signal:
                signals.append(ce_signal)

            # Generate PE signals (bearish)
            pe_signal = self._evaluate_pe_conditions(
                pcr_value, current_rsi, previous_rsi, oi_analysis, spot_price, atm_strike, symbol
            )
            if pe_signal:
                signals.append(pe_signal)

            logger.debug(f"Market analysis complete: PCR={pcr_value:.3f}, RSI={current_rsi:.1f}, "
                        f"ATM={atm_strike}, signals_generated={len(signals)}")

        except Exception as e:
            logger.error(f"Error analyzing market conditions: {e}")

        return signals

    def _analyze_oi_patterns(self, option_chain_data, spot_price, atm_strike):
        """Analyze Open Interest patterns around ATM strike."""
        try:
            # Focus on strikes around ATM (±3 strikes = ±150 points for NIFTY)
            strike_range = 3 * 50  # 3 strikes × 50 point intervals
            lower_bound = atm_strike - strike_range
            upper_bound = atm_strike + strike_range

            # Filter data for analysis window
            window_data = option_chain_data[
                (option_chain_data['strike_price'] >= lower_bound) &
                (option_chain_data['strike_price'] <= upper_bound)
            ].copy()

            if window_data.empty:
                logger.warning("No option chain data in analysis window")
                return {'ce_oi_total': 0, 'pe_oi_total': 0, 'ce_oi_change': 0, 'pe_oi_change': 0}

            # Separate CE and PE data
            ce_data = window_data[window_data['option_type'] == 'CE']
            pe_data = window_data[window_data['option_type'] == 'PE']

            # Calculate totals
            ce_oi_total = ce_data['open_interest'].sum() if not ce_data.empty else 0
            pe_oi_total = pe_data['open_interest'].sum() if not pe_data.empty else 0

            # Calculate average change in OI
            ce_oi_change = ce_data['change_in_oi'].mean() if not ce_data.empty else 0
            pe_oi_change = pe_data['change_in_oi'].mean() if not pe_data.empty else 0

            # Calculate change percentages (handle division by zero)
            ce_oi_change_pct = (ce_oi_change / ce_oi_total * 100) if ce_oi_total > 0 else 0
            pe_oi_change_pct = (pe_oi_change / pe_oi_total * 100) if pe_oi_total > 0 else 0

            oi_analysis = {
                'ce_oi_total': ce_oi_total,
                'pe_oi_total': pe_oi_total,
                'ce_oi_change': ce_oi_change,
                'pe_oi_change': pe_oi_change,
                'ce_oi_change_pct': ce_oi_change_pct,
                'pe_oi_change_pct': pe_oi_change_pct,
                'total_oi': ce_oi_total + pe_oi_total
            }

            logger.debug(f"OI Analysis: CE_OI={ce_oi_total:.0f}, PE_OI={pe_oi_total:.0f}, "
                        f"CE_change={ce_oi_change_pct:.1f}%, PE_change={pe_oi_change_pct:.1f}%")

            return oi_analysis

        except Exception as e:
            logger.error(f"Error analyzing OI patterns: {e}")
            return {'ce_oi_total': 0, 'pe_oi_total': 0, 'ce_oi_change': 0, 'pe_oi_change': 0}

    def _evaluate_ce_conditions(self, pcr_value, current_rsi, previous_rsi, oi_analysis, spot_price, atm_strike, symbol):
        """Evaluate conditions for BUY CE signal."""
        try:
            # Check if signal renewal is enabled
            if hasattr(self.config, 'enable_signal_renewal') and self.config.enable_signal_renewal:
                # Use expiry-based logic: only generate if no active signal or active signal has expired
                if self.is_signal_active('BUY_CE'):
                    logger.debug("BUY_CE signal still active, skipping generation")
                    return None
            else:
                # Fallback to original cooldown logic if renewal is disabled
                if not self._check_signal_cooldown('BUY_CE'):
                    return None

            # Signal components with weights
            signal_components = {
                'pcr_bullish': 0,
                'rsi_recovery': 0,
                'oi_supportive': 0
            }

            # 1. PCR Analysis (33% weight)
            if self.config.is_pcr_bullish(pcr_value):
                signal_components['pcr_bullish'] = 1.0
                pcr_strength = max(0, (self.config.pcr_buy_ce_max - pcr_value) / self.config.pcr_buy_ce_max)
                signal_components['pcr_bullish'] = min(1.0, 0.5 + pcr_strength)

            # 2. RSI Recovery Analysis (33% weight)
            if self.config.is_rsi_oversold_recovery(current_rsi, previous_rsi):
                signal_components['rsi_recovery'] = 1.0
            elif current_rsi <= self.config.rsi_oversold_recovery and current_rsi > previous_rsi:
                # Partial credit for RSI moving up even if not full recovery
                signal_components['rsi_recovery'] = 0.6

            # 3. OI Supportive Analysis (33% weight)
            if (oi_analysis['ce_oi_change_pct'] > 0 and
                self.config.is_oi_change_significant(oi_analysis['ce_oi_change_pct']) and
                self.config.is_oi_level_sufficient(oi_analysis['ce_oi_total'])):
                signal_components['oi_supportive'] = 1.0
            elif oi_analysis['ce_oi_change_pct'] > 0:
                # Partial credit for positive CE OI change
                signal_components['oi_supportive'] = 0.5

            # Calculate overall confidence
            confidence_score = np.mean(list(signal_components.values()))

            # Require at least 2 of 3 conditions to be partially met
            conditions_met = sum(1 for score in signal_components.values() if score > 0)
            if conditions_met < 2:
                logger.debug(f"CE signal conditions not met: {signal_components}")
                return None

            # Determine signal strength
            if confidence_score >= 0.8:
                signal_strength = 'HIGH'
            elif confidence_score >= 0.6:
                signal_strength = 'MEDIUM'
            else:
                signal_strength = 'LOW'

            # Calculate strike price (ATM or slightly OTM)
            ce_strike = atm_strike  # Start with ATM

            # Estimate premium and targets (simplified)
            premium_estimate = max(50, (spot_price - ce_strike) + 30)  # Basic intrinsic + time value
            target_price = premium_estimate * 1.33  # 33% target
            stop_loss = premium_estimate * 0.8    # 20% stop loss

            # Create signal
            signal = {
                'signal_type': 'BUY_CE',
                'strike_price': ce_strike,
                'signal_strength': signal_strength,
                'confidence_score': confidence_score,
                'pcr_value': pcr_value,
                'rsi_value': current_rsi,
                'oi_change_pct': oi_analysis['ce_oi_change_pct'],
                'spot_price': spot_price,
                'premium_price': premium_estimate,
                'target_price': target_price,
                'stop_loss': stop_loss,
                'generated_at': datetime.now(),
                'symbol': symbol,
                'validity_minutes': self.config.signal_cooldown_minutes,
                'market_context': self._generate_market_context(signal_components, 'BULLISH')
            }

            logger.info(f"CE signal generated: strike={ce_strike}, confidence={confidence_score:.2f}, "
                       f"components={signal_components}")

            return signal

        except Exception as e:
            logger.error(f"Error evaluating CE conditions: {e}")
            return None

    def _evaluate_pe_conditions(self, pcr_value, current_rsi, previous_rsi, oi_analysis, spot_price, atm_strike, symbol):
        """Evaluate conditions for BUY PE signal."""
        try:
            # Check if signal renewal is enabled
            if hasattr(self.config, 'enable_signal_renewal') and self.config.enable_signal_renewal:
                # Use expiry-based logic: only generate if no active signal or active signal has expired
                if self.is_signal_active('BUY_PE'):
                    logger.debug("BUY_PE signal still active, skipping generation")
                    return None
            else:
                # Fallback to original cooldown logic if renewal is disabled
                if not self._check_signal_cooldown('BUY_PE'):
                    return None

            # Signal components with weights
            signal_components = {
                'pcr_bearish': 0,
                'rsi_decline': 0,
                'oi_supportive': 0
            }

            # 1. PCR Analysis (33% weight)
            if self.config.is_pcr_bearish(pcr_value):
                signal_components['pcr_bearish'] = 1.0
                pcr_strength = (pcr_value - self.config.pcr_buy_pe_min) / self.config.pcr_buy_pe_min
                signal_components['pcr_bearish'] = min(1.0, 0.5 + pcr_strength)

            # 2. RSI Decline Analysis (33% weight)
            if self.config.is_rsi_overbought_decline(current_rsi, previous_rsi):
                signal_components['rsi_decline'] = 1.0
            elif current_rsi >= self.config.rsi_overbought_decline and current_rsi < previous_rsi:
                # Partial credit for RSI moving down even if not full decline
                signal_components['rsi_decline'] = 0.6

            # 3. OI Supportive Analysis (33% weight)
            if (oi_analysis['pe_oi_change_pct'] > 0 and
                self.config.is_oi_change_significant(oi_analysis['pe_oi_change_pct']) and
                self.config.is_oi_level_sufficient(oi_analysis['pe_oi_total'])):
                signal_components['oi_supportive'] = 1.0
            elif oi_analysis['pe_oi_change_pct'] > 0:
                # Partial credit for positive PE OI change
                signal_components['oi_supportive'] = 0.5

            # Calculate overall confidence
            confidence_score = np.mean(list(signal_components.values()))

            # Require at least 2 of 3 conditions to be partially met
            conditions_met = sum(1 for score in signal_components.values() if score > 0)
            if conditions_met < 2:
                logger.debug(f"PE signal conditions not met: {signal_components}")
                return None

            # Determine signal strength
            if confidence_score >= 0.8:
                signal_strength = 'HIGH'
            elif confidence_score >= 0.6:
                signal_strength = 'MEDIUM'
            else:
                signal_strength = 'LOW'

            # Calculate strike price (ATM or slightly OTM)
            pe_strike = atm_strike  # Start with ATM

            # Estimate premium and targets (simplified)
            premium_estimate = max(50, (pe_strike - spot_price) + 30)  # Basic intrinsic + time value
            target_price = premium_estimate * 1.33  # 33% target
            stop_loss = premium_estimate * 0.8    # 20% stop loss

            # Create signal
            signal = {
                'signal_type': 'BUY_PE',
                'strike_price': pe_strike,
                'signal_strength': signal_strength,
                'confidence_score': confidence_score,
                'pcr_value': pcr_value,
                'rsi_value': current_rsi,
                'oi_change_pct': oi_analysis['pe_oi_change_pct'],
                'spot_price': spot_price,
                'premium_price': premium_estimate,
                'target_price': target_price,
                'stop_loss': stop_loss,
                'generated_at': datetime.now(),
                'symbol': symbol,
                'validity_minutes': self.config.signal_cooldown_minutes,
                'market_context': self._generate_market_context(signal_components, 'BEARISH')
            }

            logger.info(f"PE signal generated: strike={pe_strike}, confidence={confidence_score:.2f}, "
                       f"components={signal_components}")

            return signal

        except Exception as e:
            logger.error(f"Error evaluating PE conditions: {e}")
            return None

    def _generate_market_context(self, signal_components, direction):
        """Generate market context description based on signal components."""
        context_parts = []

        if signal_components.get('pcr_bullish', 0) > 0.7:
            context_parts.append("Strong PCR bullish")
        elif signal_components.get('pcr_bearish', 0) > 0.7:
            context_parts.append("Strong PCR bearish")

        if signal_components.get('rsi_recovery', 0) > 0.7:
            context_parts.append("RSI recovery")
        elif signal_components.get('rsi_decline', 0) > 0.7:
            context_parts.append("RSI decline")

        if signal_components.get('oi_supportive', 0) > 0.7:
            context_parts.append("OI buildup")

        if not context_parts:
            return f"{direction.capitalize()} momentum"

        return " + ".join(context_parts)

    def _check_signal_cooldown(self, signal_type):
        """Check if enough time has passed since last signal of this type."""
        if signal_type not in self.last_signals:
            return True

        last_time = self.last_signals[signal_type]
        cooldown_delta = timedelta(minutes=self.config.signal_cooldown_minutes)

        return datetime.now() - last_time >= cooldown_delta

    def _check_rate_limits(self):
        """Check if we're within the hourly rate limit."""
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)

        # Clean old signals from history
        self.signal_history = [
            signal_time for signal_time in self.signal_history
            if signal_time > one_hour_ago
        ]

        return len(self.signal_history) < self.config.max_signals_per_hour

    def _save_signal_to_database(self, signal):
        """Save signal to database for history tracking."""
        try:
            success = save_trading_signal(signal)
            if success:
                logger.debug(f"Signal saved to database: {signal['signal_type']} at {signal['strike_price']}")
            else:
                logger.warning(f"Failed to save signal to database: {signal['signal_type']}")
        except Exception as e:
            logger.error(f"Error saving signal to database: {e}")

    def _track_signal_for_rate_limiting(self, signal):
        """Track signal for rate limiting and cooldown."""
        signal_type = signal['signal_type']
        generated_at = signal['generated_at']

        # Update last signal time for cooldown
        self.last_signals[signal_type] = generated_at

        # Add to history for rate limiting
        self.signal_history.append(generated_at)

    def track_active_signal(self, signal):
        """Track a newly generated signal as active with expiry time."""
        try:
            signal_type = signal['signal_type']
            generated_at = signal['generated_at']
            validity_minutes = signal.get('validity_minutes', self.config.signal_cooldown_minutes)

            # Calculate expiry time
            expiry_time = generated_at + timedelta(minutes=validity_minutes)

            # Store signal with expiry time
            self.active_signals[signal_type] = {
                'signal_data': signal.copy(),
                'generated_at': generated_at,
                'expiry_time': expiry_time,
                'validity_minutes': validity_minutes
            }

            logger.info(f"Tracking active signal: {signal_type} at {signal.get('strike_price')}, expires at {expiry_time}")

        except Exception as e:
            logger.error(f"Error tracking active signal: {e}")

    def get_active_signals(self):
        """Get currently active signals."""
        return self.active_signals.copy()

    def clear_expired_signals(self):
        """Remove expired signals from active tracking."""
        try:
            current_time = datetime.now()
            expired_types = []

            for signal_type, signal_info in self.active_signals.items():
                expiry_time = signal_info['expiry_time']

                # Check if signal has expired (with optional buffer from config)
                buffer_seconds = getattr(self.config, 'signal_expiry_buffer_seconds', 0)
                expiry_with_buffer = expiry_time + timedelta(seconds=buffer_seconds)

                if current_time >= expiry_with_buffer:
                    expired_types.append(signal_type)
                    logger.info(f"Signal expired: {signal_type} at {signal_info['signal_data'].get('strike_price')}, "
                              f"expired at {expiry_time}")

            # Remove expired signals
            for signal_type in expired_types:
                del self.active_signals[signal_type]

            if expired_types:
                logger.debug(f"Cleared {len(expired_types)} expired signals: {expired_types}")

        except Exception as e:
            logger.error(f"Error clearing expired signals: {e}")

    def is_signal_active(self, signal_type):
        """Check if a signal of the given type is currently active."""
        try:
            if signal_type not in self.active_signals:
                return False

            # Check if the signal has expired (call clear_expired_signals for consistency)
            self.clear_expired_signals()

            # After cleanup, check again
            return signal_type in self.active_signals

        except Exception as e:
            logger.error(f"Error checking signal active status: {e}")
            return False

    def is_signal_expired(self, signal_type):
        """Check if a signal of the given type has expired."""
        try:
            if signal_type not in self.active_signals:
                return True  # No active signal means it's effectively expired

            current_time = datetime.now()
            signal_info = self.active_signals[signal_type]
            expiry_time = signal_info['expiry_time']

            return current_time >= expiry_time

        except Exception as e:
            logger.error(f"Error checking signal expiry: {e}")
            return True  # Assume expired on error for safety

    def get_signal_status(self):
        """Get current status of signal engine."""
        recent_signals = len(self.signal_history)
        rate_limit_remaining = max(0, self.config.max_signals_per_hour - recent_signals)

        # Get active signals info
        active_signals_info = {}
        for signal_type, signal_info in self.active_signals.items():
            active_signals_info[signal_type] = {
                'strike_price': signal_info['signal_data'].get('strike_price'),
                'generated_at': signal_info['generated_at'],
                'expiry_time': signal_info['expiry_time'],
                'time_remaining': max(0, (signal_info['expiry_time'] - datetime.now()).total_seconds())
            }

        status = {
            'enabled': self.config.enabled,
            'market_hours': self.config.is_market_hours(),
            'recent_signals_count': recent_signals,
            'rate_limit_remaining': rate_limit_remaining,
            'last_signals': self.last_signals.copy(),
            'active_signals': active_signals_info,
            'active_signals_count': len(self.active_signals),
            'signal_renewal_enabled': getattr(self.config, 'enable_signal_renewal', False),
            'configuration': self.config.get_config_summary()
        }

        return status


# Global engine instance
_signal_engine = None


def get_signal_engine():
    """Get global signal engine instance."""
    global _signal_engine
    if _signal_engine is None:
        _signal_engine = AdvancedSignalEngine()
    return _signal_engine


def generate_advanced_signals(symbol="NIFTY"):
    """Generate signals using the advanced engine."""
    return get_signal_engine().generate_signals(symbol)


if __name__ == "__main__":
    # Test signal generation
    logger.info("Testing advanced signal engine...")

    try:
        engine = get_signal_engine()
        signals = engine.generate_signals("NIFTY")

        if signals:
            print(f"\nGenerated {len(signals)} signals:")
            for i, signal in enumerate(signals, 1):
                print(f"\nSignal {i}:")
                print(f"  Type: {signal['signal_type']}")
                print(f"  Strike: {signal['strike_price']}")
                print(f"  Confidence: {signal['confidence_score']:.2f}")
                print(f"  Strength: {signal['signal_strength']}")
                print(f"  Context: {signal['market_context']}")

                # Format message
                formatter = get_signal_formatter()
                message = formatter.format_signal_message(signal)
                print(f"\nFormatted Message:\n{message}")
        else:
            print("No signals generated")

        # Print status
        status = engine.get_signal_status()
        print(f"\nEngine Status:")
        print(f"  Enabled: {status['enabled']}")
        print(f"  Market Hours: {status['market_hours']}")
        print(f"  Recent Signals: {status['recent_signals_count']}")
        print(f"  Rate Limit Remaining: {status['rate_limit_remaining']}")

    except Exception as e:
        logger.error(f"Error testing signal engine: {e}", exc_info=True)
        print(f"Error: {e}")