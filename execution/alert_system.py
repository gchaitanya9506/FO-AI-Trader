import requests
import time
from strategy.signal_engine import generate_signal
from config.secrets import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from config.logging_config import get_logger

logger = get_logger('api')

def send_telegram_message(msg, max_retries=3, retry_delay=1.0, timeout=10):
    """Send message to Telegram with retry logic and error handling."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}

    for attempt in range(max_retries):
        try:
            logger.info(f"Sending Telegram message (attempt {attempt + 1}/{max_retries})")
            response = requests.post(
                url,
                data=data,
                timeout=timeout
            )

            # Check if request was successful
            if response.status_code == 200:
                logger.info("Telegram message sent successfully")
                return True
            elif response.status_code == 429:
                # Rate limit exceeded - wait longer
                retry_after = response.headers.get('Retry-After', retry_delay * 2)
                logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                time.sleep(float(retry_after))
                continue
            else:
                logger.warning(f"Telegram API returned status code {response.status_code}: {response.text}")

        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error on attempt {attempt + 1}: {e}")
        except requests.exceptions.Timeout as e:
            logger.warning(f"Timeout error on attempt {attempt + 1}: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error on attempt {attempt + 1}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")

        # Wait before retrying (except on last attempt)
        if attempt < max_retries - 1:
            wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
            logger.info(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)

    logger.error(f"Failed to send Telegram message after {max_retries} attempts")
    return False

def send_alert(signal):
    """Send trading signal alert with proper error handling."""
    try:
        message = f"🤖 AI TRADING SIGNAL: {signal}\n\nTime: {time.strftime('%Y-%m-%d %H:%M:%S')}"
        success = send_telegram_message(message)

        if not success:
            logger.error("Failed to send alert - all retry attempts exhausted")
            # Could implement fallback notification method here (email, file log, etc.)

        return success

    except Exception as e:
        logger.error(f"Error in send_alert: {e}")
        return False

# Legacy function for backwards compatibility
def send(msg):
    """Legacy send function - use send_alert() for new code."""
    logger.warning("Using deprecated send() function. Use send_alert() instead.")
    return send_telegram_message(msg)

if __name__ == "__main__":
    try:
        signal = generate_signal()
        send_alert(signal)
    except Exception as e:
        logger.error(f"Error generating or sending signal: {e}")