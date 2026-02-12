# fetch_data.py
import os
from datetime import datetime, date, timedelta
import time
import requests
import pandas as pd
import yfinance as yf

# Optional libraries: nsepython or nsepy. Use whichever you have installed.
try:
    import nsepython
    HAVE_NSEPYTHON = True
except Exception as e:
    print("nsepython import error:", e)
    HAVE_NSEPYTHON = False

try:
    from nsepy import get_history
    HAVE_NSEPY = True
except Exception:
    HAVE_NSEPY = False

def fetch_nifty_data(interval="15m", ticker="^NSEI", period="60d", save_path="data/raw/nifty_data.csv", max_retries=3, retry_delay=2.0):
    """Fetch NIFTY data with retry logic and error handling."""
    from config.logging_config import get_logger

    logger = get_logger('api')

    logger.info(f"[fetch_nifty_data] Fetching {ticker} interval={interval} period={period} ...")

    for attempt in range(max_retries):
        try:
            logger.info(f"YFinance API call attempt {attempt + 1}/{max_retries}")

            # Rate limiting delay
            time.sleep(2)

            df = yf.download(ticker, interval=interval, period=period, progress=False, threads=False)

            if df is None or df.empty:
                logger.warning(f"[fetch_nifty_data] No data fetched on attempt {attempt + 1}. Check ticker/interval/period.")
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error("[fetch_nifty_data] Failed to fetch data after all retries.")
                    return None

            # normalize index -> Datetime column
            df = df.reset_index().rename(columns={"Date": "Datetime"})
            # Ensure a Datetime col with timezone-naive UTC
            df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_convert(None) if df['Datetime'].dt.tz is not None else pd.to_datetime(df['Datetime'])

            # Validate price data quality
            from utils.data_validation import clean_and_validate_data

            df_clean, quality_report = clean_and_validate_data(df, "price")
            if not quality_report.passed:
                logger.error(f"NIFTY price data quality validation failed:\n{quality_report.summary()}")
                return None
            elif quality_report.warnings:
                logger.warning(f"NIFTY price data quality warnings:\n{quality_report.summary()}")

            df = df_clean
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            df.to_csv(save_path, index=False)
            logger.info(f"[fetch_nifty_data] Successfully saved {len(df)} rows to {save_path}")
            return df

        except Exception as e:
            logger.error(f"Error fetching NIFTY data on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                logger.error("All retry attempts exhausted for YFinance API")
                return None
            else:
                wait_time = retry_delay * (2 ** attempt)
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

    return None

def fetch_option_chain_nse(symbol="NIFTY", save_path="data/raw/nifty_option_chain.csv", max_retries=3, retry_delay=2.0):
    """Fetch NSE option chain with retry logic and error handling."""
    from config.logging_config import get_logger

    logger = get_logger('api')

    logger.info(f"[fetch_option_chain_nse] Fetching option chain for {symbol}...")

    if not HAVE_NSEPYTHON:
        logger.error("[fetch_option_chain_nse] nsepython is not available")
        return None

    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

    for attempt in range(max_retries):
        try:
            logger.info(f"NSE API call attempt {attempt + 1}/{max_retries}")

            # Add rate limiting delay
            if attempt > 0:
                wait_time = retry_delay * (2 ** (attempt - 1))  # Exponential backoff
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

            # nsepython handles cookies/headers internally
            data = nsepython.nsefetch(url)

            if not data:
                logger.warning(f"Empty response from NSE API on attempt {attempt + 1}")
                continue

            records = data.get("records", {}).get("data", [])
            logger.info(f"[debug] records length: {len(records)}")

            if not records:
                logger.warning(f"[fetch_option_chain_nse] No records found on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    continue
                else:
                    logger.error("[fetch_option_chain_nse] No rows parsed from NSE after all retries.")
                    return None

            # If we got here, we have valid data
            break

        except Exception as e:
            logger.error(f"Error fetching NSE data on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                logger.error("All retry attempts exhausted for NSE API")
                return None
            continue

    else:
        logger.error("Failed to fetch data after all retry attempts")
        return None

    # Validate NSE API response
    from utils.data_validation import validate_nse_option_chain_data, clean_and_validate_data

    validation_report = validate_nse_option_chain_data(data)
    if not validation_report.passed:
        logger.error(f"NSE API response validation failed:\n{validation_report.summary()}")
        return None
    elif validation_report.warnings:
        logger.warning(f"NSE API response warnings:\n{validation_report.summary()}")

    rows = []
    today = datetime.now()

    for rec in records:
        strike = rec.get("strikePrice")
        expiry = rec.get("expiryDate")

        for side in ("CE", "PE"):
            side_data = rec.get(side)
            if side_data:
                rows.append({
                    "Strike Price": strike,
                    "Option Type": side,
                    "Last Price": side_data.get("lastPrice"),
                    "IV": side_data.get("impliedVolatility"),
                    "Open Interest": side_data.get("openInterest"),
                    "Change in OI": side_data.get("changeinOpenInterest"),
                    "Date": today,
                    "Expiry": expiry
                })

    df = pd.DataFrame(rows)

    # Clean and validate the DataFrame
    df_clean, quality_report = clean_and_validate_data(df, "option")
    if not quality_report.passed:
        logger.error(f"Option chain data quality validation failed:\n{quality_report.summary()}")
        return None
    elif quality_report.warnings:
        logger.warning(f"Option chain data quality warnings:\n{quality_report.summary()}")

    df = df_clean
    df["Date"] = pd.to_datetime(df["Date"])
    df["Expiry"] = pd.to_datetime(df["Expiry"], errors="coerce")

    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)

    print(f"[fetch_option_chain_nse] Saved to {save_path}")
    return df

def get_next_expiry_date(from_date=None):
    """Get the next reasonable expiry date (typically next Thursday for NIFTY)."""
    if from_date is None:
        from_date = date.today()

    # Find next Thursday (NIFTY options typically expire on Thursdays)
    days_ahead = 3 - from_date.weekday()  # Thursday is weekday 3
    if days_ahead <= 0:  # Target day already happened this week
        days_ahead += 7

    next_expiry = from_date + timedelta(days=days_ahead)
    return next_expiry


def fetch_option_chain_get_history(symbol="NIFTY", start_date=None, end_date=None, strikes=None, expiry_date=None, save_path="data/raw/nifty_option_chain.csv"):
    """Fallback: iterate get_history for a few strikes. This can be slow and may need credentials/API access."""
    from config.logging_config import get_logger

    logger = get_logger('api')

    if not HAVE_NSEPY:
        logger.error("[fetch_option_chain_get_history] nsepy not available")
        return None

    logger.info("[fetch_option_chain_get_history] Using nsepy.get_history fallback...")

    if start_date is None:
        start_date = date.today() - timedelta(days=30)
    if end_date is None:
        end_date = date.today()
    if expiry_date is None:
        expiry_date = get_next_expiry_date()
        logger.info(f"Using dynamic expiry date: {expiry_date}")

    # strikes is a list of strike prices to pull; if not provided build around ATM later
    rows = []
    if strikes is None:
        # sample strike offsets; user should refine
        strikes = [20000, 20500, 21000]  # placeholder; replace with dynamic ATM logic
    for sp in strikes:
        for opt_type in ("CE", "PE"):
            try:
                hist = get_history(symbol=symbol, start=start_date, end=end_date, option_type=opt_type, strike_price=sp, expiry_date=expiry_date)
                if hist is None or hist.empty:
                    continue
                hist = hist.reset_index()
                hist['Option Type'] = opt_type
                hist['Strike Price'] = sp
                # normalize Date column name
                if 'Date' in hist.columns:
                    hist.rename(columns={'Date': 'Date'}, inplace=True)
                rows.append(hist)
            except Exception as e:
                print(f"[fetch_option_chain_get_history] error for strike {sp} {opt_type}: {e}")
    if not rows:
        print("[fetch_option_chain_get_history] No data collected.")
        return None
    df = pd.concat(rows, ignore_index=True, sort=False)
    # ensure Date and Expiry exist as datetimes (nsepy returns date objects)
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
    if 'Expiry' in df.columns:
        df['Expiry'] = pd.to_datetime(df['Expiry'], errors='coerce')
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path, index=False)
    print(f"[fetch_option_chain_get_history] Saved to {save_path}")
    return df

if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    # Underlying (multiple intervals saved for convenience)
    # fetch_nifty_data(interval="15m", period="60d", save_path="data/raw/nifty_data_15m.csv")
    fetch_nifty_data(interval="5m", period="30d", save_path="data/raw/nifty_data_5m.csv")
    # fetch_nifty_data(interval="30m", period="90d", save_path="data/raw/nifty_data_30m.csv")

    # Option chain (try preferred method then fallback)
    # oc = None
    # if HAVE_NSEPYTHON:
    #     oc = fetch_option_chain_nsepython("NIFTY", save_path="data/raw/nifty_option_chain.csv")
    # if oc is None and HAVE_NSEPY:
    #     # example expiry guess; adjust dynamically as needed
    #     expiry_date = None
    #     oc = fetch_option_chain_get_history(symbol="NIFTY", expiry_date=expiry_date, save_path="data/raw/nifty_option_chain.csv")
    # if oc is None:
    #     print("[main] No option chain available. Install nsepython or nsepy or provide option CSV manually.")
    oc = fetch_option_chain_nse("NIFTY", save_path="data/raw/nifty_option_chain.csv")

    if oc is None:
        print("[main] Failed to fetch option chain.")