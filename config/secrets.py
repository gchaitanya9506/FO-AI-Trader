import os
from dotenv import load_dotenv

load_dotenv()

def validate_environment():
    """Validate that all required environment variables are set."""
    required_vars = {
        'TELEGRAM_TOKEN': 'Telegram bot token is required',
        'TELEGRAM_CHAT_ID': 'Telegram chat ID is required'
    }

    missing_vars = []
    for var, description in required_vars.items():
        if not os.getenv(var):
            missing_vars.append(f"{var}: {description}")

    if missing_vars:
        error_msg = "Missing required environment variables:\n" + "\n".join(f"  - {var}" for var in missing_vars)
        error_msg += "\n\nPlease check your .env file or set these environment variables."
        error_msg += "\nSee .env.example for required format."
        raise EnvironmentError(error_msg)

# Validate environment on import
validate_environment()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")