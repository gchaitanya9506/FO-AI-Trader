"""
Enhanced Telegram Message Formatting for CE/PE Trading Signals
Creates rich, contextual messages with strike prices, market data, and technical levels.
"""

from datetime import datetime, timedelta
from config.logging_config import get_logger

logger = get_logger('signal_formatter')


class SignalFormatter:
    """Enhanced formatter for CE/PE trading signals with market context."""

    def __init__(self):
        self.emoji_map = {
            'BUY_CE': '🚀',
            'BUY_PE': '🔻',
            'HIGH': '🔥',
            'MEDIUM': '⚡',
            'LOW': '💫',
            'BULLISH': '📈',
            'BEARISH': '📉',
            'NEUTRAL': '➡️'
        }

    def format_signal_message(self, signal_data):
        """
        Format a comprehensive trading signal message for Telegram.

        Args:
            signal_data: Dictionary containing signal information

        Returns:
            Formatted message string
        """
        try:
            signal_type = signal_data.get('signal_type', 'UNKNOWN')
            strike_price = signal_data.get('strike_price', 0)
            premium_price = signal_data.get('premium_price', 0)
            confidence_score = signal_data.get('confidence_score', 0)
            signal_strength = signal_data.get('signal_strength', 'MEDIUM')

            # Header with emoji and signal type
            header_emoji = self.emoji_map.get(signal_type, '📊')
            strength_emoji = self.emoji_map.get(signal_strength, '⚡')

            # Format header
            header = f"{header_emoji} {signal_type.replace('_', ' ')} {int(strike_price)}"
            if premium_price:
                header += f" @ ₹{premium_price:.1f}"

            # Build message parts
            message_parts = [
                header,
                self._format_technical_section(signal_data),
                self._format_targets_section(signal_data),
                self._format_context_section(signal_data, strength_emoji),
                self._format_timing_section(signal_data)
            ]

            # Join non-empty parts
            message = '\n'.join(part for part in message_parts if part)

            logger.debug(f"Formatted signal message for {signal_type} at {strike_price}")
            return message

        except Exception as e:
            logger.error(f"Error formatting signal message: {e}")
            return self._format_simple_fallback(signal_data)

    def _format_technical_section(self, signal_data):
        """Format technical indicators section."""
        pcr_value = signal_data.get('pcr_value', 0)
        rsi_value = signal_data.get('rsi_value', 0)
        oi_change_pct = signal_data.get('oi_change_pct', 0)

        technical_parts = []

        if pcr_value:
            # PCR with trend indication
            if pcr_value <= 0.7:
                pcr_trend = "📈 Bullish"
            elif pcr_value >= 1.3:
                pcr_trend = "📉 Bearish"
            else:
                pcr_trend = "➡️ Neutral"

            technical_parts.append(f"PCR: {pcr_value:.2f} {pcr_trend}")

        if rsi_value:
            # RSI with condition
            if rsi_value <= 30:
                rsi_condition = "(Oversold)"
            elif rsi_value >= 70:
                rsi_condition = "(Overbought)"
            else:
                rsi_condition = ""

            technical_parts.append(f"RSI: {rsi_value:.1f} {rsi_condition}")

        if oi_change_pct:
            oi_direction = "↗️" if oi_change_pct > 0 else "↘️"
            technical_parts.append(f"OI Change: {oi_direction}{abs(oi_change_pct):.1f}%")

        if technical_parts:
            return f"📊 {' | '.join(technical_parts)}"
        return ""

    def _format_targets_section(self, signal_data):
        """Format targets and stop loss section."""
        target_price = signal_data.get('target_price')
        stop_loss = signal_data.get('stop_loss')

        if target_price or stop_loss:
            targets_parts = []
            if target_price:
                targets_parts.append(f"Target: ₹{target_price:.1f}")
            if stop_loss:
                targets_parts.append(f"SL: ₹{stop_loss:.1f}")

            return f"🎯 {' | '.join(targets_parts)}"
        return ""

    def _format_context_section(self, signal_data, strength_emoji):
        """Format market context and confidence section."""
        confidence_score = signal_data.get('confidence_score', 0)
        spot_price = signal_data.get('spot_price')
        market_context = signal_data.get('market_context', '')

        context_parts = []

        # Confidence with visual indicator
        confidence_pct = int(confidence_score * 100)
        confidence_bars = "█" * (confidence_pct // 20)
        confidence_empty = "░" * (5 - (confidence_pct // 20))
        context_parts.append(f"Confidence: {confidence_pct}% {confidence_bars}{confidence_empty}")

        # Spot price if available
        if spot_price:
            context_parts.append(f"Spot: ₹{spot_price:.1f}")

        # Market context
        if market_context:
            context_parts.append(market_context)

        if context_parts:
            return f"{strength_emoji} {' | '.join(context_parts)}"
        return ""

    def _format_timing_section(self, signal_data):
        """Format timing and validity section."""
        generated_at = signal_data.get('generated_at', datetime.now())
        validity_minutes = signal_data.get('validity_minutes', 15)

        # Calculate validity time
        if isinstance(generated_at, str):
            try:
                generated_at = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
            except:
                generated_at = datetime.now()

        valid_until = generated_at + timedelta(minutes=validity_minutes)

        time_parts = [
            f"Generated: {generated_at.strftime('%H:%M:%S')}",
            f"Valid till: {valid_until.strftime('%H:%M')}"
        ]

        # Urgency indicator based on signal strength
        signal_strength = signal_data.get('signal_strength', 'MEDIUM')
        urgency_map = {'HIGH': 'HIGH ⚡', 'MEDIUM': 'MEDIUM', 'LOW': 'LOW'}
        urgency = urgency_map.get(signal_strength, 'MEDIUM')

        time_parts.append(f"Urgency: {urgency}")

        return f"⏰ {' | '.join(time_parts)}"

    def _format_simple_fallback(self, signal_data):
        """Simple fallback format if main formatting fails."""
        signal_type = signal_data.get('signal_type', 'SIGNAL')
        strike_price = signal_data.get('strike_price', 'N/A')
        confidence = signal_data.get('confidence_score', 0)

        return f"""🤖 {signal_type.replace('_', ' ')} {strike_price}
⚡ Confidence: {int(confidence * 100)}%
⏰ {datetime.now().strftime('%H:%M:%S')}"""

    def format_alert_summary(self, signals_list):
        """Format a summary of multiple signals."""
        if not signals_list:
            return "📊 No signals generated"

        if len(signals_list) == 1:
            return self.format_signal_message(signals_list[0])

        # Multiple signals summary
        ce_signals = [s for s in signals_list if s.get('signal_type') == 'BUY_CE']
        pe_signals = [s for s in signals_list if s.get('signal_type') == 'BUY_PE']

        summary_parts = [f"📊 SIGNAL BATCH ({len(signals_list)} signals)"]

        if ce_signals:
            ce_strikes = [str(int(s.get('strike_price', 0))) for s in ce_signals]
            summary_parts.append(f"🚀 CE: {', '.join(ce_strikes)}")

        if pe_signals:
            pe_strikes = [str(int(s.get('strike_price', 0))) for s in pe_signals]
            summary_parts.append(f"🔻 PE: {', '.join(pe_strikes)}")

        # Add timing
        summary_parts.append(f"⏰ {datetime.now().strftime('%H:%M:%S')}")

        return '\n'.join(summary_parts)

    def format_error_message(self, error_context=""):
        """Format error notification message."""
        error_msg = "❌ Signal Generation Error"
        if error_context:
            error_msg += f"\n🔍 Context: {error_context}"
        error_msg += f"\n⏰ {datetime.now().strftime('%H:%M:%S')}"
        return error_msg

    def format_status_message(self, status, context=""):
        """Format system status message."""
        status_emojis = {
            'STARTED': '✅',
            'STOPPED': '🛑',
            'PAUSED': '⏸️',
            'ERROR': '❌',
            'WARNING': '⚠️'
        }

        emoji = status_emojis.get(status, '📊')
        message = f"{emoji} Signal Monitor: {status}"

        if context:
            message += f"\n{context}"

        message += f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        return message


# Global formatter instance
_formatter = None


def get_signal_formatter():
    """Get global signal formatter instance."""
    global _formatter
    if _formatter is None:
        _formatter = SignalFormatter()
    return _formatter


# Convenience functions for backward compatibility
def format_signal(signal_data):
    """Format a single signal message."""
    return get_signal_formatter().format_signal_message(signal_data)


def format_multiple_signals(signals_list):
    """Format multiple signals as a summary."""
    return get_signal_formatter().format_alert_summary(signals_list)


if __name__ == "__main__":
    # Test formatter with sample data
    sample_signal = {
        'signal_type': 'BUY_CE',
        'strike_price': 21000,
        'premium_price': 150,
        'confidence_score': 0.85,
        'signal_strength': 'HIGH',
        'pcr_value': 0.65,
        'rsi_value': 42,
        'oi_change_pct': 18.5,
        'spot_price': 20980,
        'target_price': 200,
        'stop_loss': 120,
        'validity_minutes': 15,
        'generated_at': datetime.now(),
        'market_context': 'Strong momentum breakout'
    }

    formatter = get_signal_formatter()
    print("Sample Signal Message:")
    print(formatter.format_signal_message(sample_signal))
    print("\n" + "="*50 + "\n")

    # Test multiple signals
    sample_signals = [
        sample_signal,
        {**sample_signal, 'signal_type': 'BUY_PE', 'strike_price': 20900, 'pcr_value': 1.45}
    ]

    print("Multiple Signals Summary:")
    print(formatter.format_alert_summary(sample_signals))