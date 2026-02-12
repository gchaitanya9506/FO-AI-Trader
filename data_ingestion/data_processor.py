# data_processor.py
"""
Automated Data Processor for NSE Option Chain
Eliminates manual CSV cleaning by integrating cleaning logic directly into the fetch process.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any
from config.logging_config import get_logger
from config.app_config import get_config

logger = get_logger('data_processor')
config = get_config()


class OptionChainProcessor:
    """
    Unified processor that handles NSE Option Chain data fetching, cleaning,
    and processing in a single automated pipeline.
    """

    def __init__(self):
        self.config = config
        self.logger = logger

    def fetch_and_process_option_chain(
        self,
        symbol: str = "NIFTY",
        save_to_database: bool = None,
        save_to_csv: bool = True,
        csv_path: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Fetch NSE option chain and process it automatically without manual intervention.

        Args:
            symbol: NSE symbol to fetch (default: NIFTY)
            save_to_database: Whether to save directly to database (default: from config)
            save_to_csv: Whether to save to CSV (default: True for backward compatibility)
            csv_path: Custom CSV path (optional)

        Returns:
            Processed DataFrame or None if failed
        """
        if save_to_database is None:
            save_to_database = self.config.data_pipeline.enable_database_direct

        self.logger.info(f"Starting automated option chain processing for {symbol}")

        try:
            # Step 1: Fetch raw option chain data from NSE
            raw_data = self._fetch_raw_option_chain_nse(symbol)
            if raw_data is None:
                self.logger.error("Failed to fetch raw option chain data")
                return None

            # Step 2: Clean and process the raw data
            processed_df = self._clean_and_process_data(raw_data, symbol)
            if processed_df is None or processed_df.empty:
                self.logger.error("Failed to process option chain data")
                return None

            # Step 3: Validate data quality
            validated_df = self._validate_processed_data(processed_df)
            if validated_df is None:
                self.logger.error("Data validation failed")
                return None

            # Step 4: Save processed data
            if save_to_database:
                success = self._save_to_database(validated_df, symbol)
                if not success:
                    self.logger.warning("Failed to save to database, will save to CSV as fallback")
                    save_to_csv = True

            if save_to_csv:
                csv_save_path = csv_path or "data/raw/nifty_option_chain_clean.csv"
                self._save_to_csv(validated_df, csv_save_path)

            self.logger.info(f"Successfully processed {len(validated_df)} option chain records")
            return validated_df

        except Exception as e:
            self.logger.error(f"Error in automated option chain processing: {e}")
            return None

    def _fetch_raw_option_chain_nse(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch raw option chain data from NSE API using the existing fetch_option_chain_nse logic.
        """
        try:
            # Import nsepython here to handle import errors gracefully
            try:
                import nsepython
            except ImportError:
                self.logger.error("nsepython is not available - install with: pip install nsepython")
                return None

            url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

            # Fetch with retry logic
            max_retries = self.config.api.max_retries
            retry_delay = self.config.api.retry_delay

            for attempt in range(max_retries):
                try:
                    self.logger.info(f"NSE API call attempt {attempt + 1}/{max_retries}")

                    # Add exponential backoff for retries
                    if attempt > 0:
                        wait_time = retry_delay * (2 ** (attempt - 1))
                        self.logger.info(f"Waiting {wait_time} seconds before retry...")
                        import time
                        time.sleep(wait_time)

                    # Use nsepython to fetch data
                    data = nsepython.nsefetch(url)

                    if not data:
                        self.logger.warning(f"Empty response from NSE API on attempt {attempt + 1}")
                        continue

                    records = data.get("records", {}).get("data", [])
                    if not records:
                        self.logger.warning(f"No records found on attempt {attempt + 1}")
                        if attempt < max_retries - 1:
                            continue
                        else:
                            self.logger.error("No records found after all retry attempts")
                            return None

                    self.logger.info(f"Successfully fetched {len(records)} raw records from NSE")
                    return data

                except Exception as e:
                    self.logger.error(f"Error on attempt {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        self.logger.error("All retry attempts exhausted")
                        return None
                    continue

        except Exception as e:
            self.logger.error(f"Error fetching raw option chain data: {e}")
            return None

        return None

    def _clean_and_process_data(self, raw_data: Dict[str, Any], symbol: str) -> Optional[pd.DataFrame]:
        """
        Clean and process raw NSE option chain data.
        Integrates the logic from scripts/clean_option_chain.py.
        """
        try:
            records = raw_data.get("records", {}).get("data", [])
            if not records:
                self.logger.error("No records to process in raw data")
                return None

            rows = []
            today = datetime.now()

            # Process each record and create separate rows for CE and PE
            for rec in records:
                strike = rec.get("strikePrice")
                expiry = rec.get("expiryDate")

                # Skip if strike price is missing
                if strike is None:
                    continue

                # Process Call Option (CE)
                ce_data = rec.get("CE")
                if ce_data:
                    rows.append({
                        "Strike Price": strike,
                        "Option Type": "CE",
                        "Last Price": ce_data.get("lastPrice", 0),
                        "IV": ce_data.get("impliedVolatility", 0),
                        "Open Interest": ce_data.get("openInterest", 0),
                        "Change in OI": ce_data.get("changeinOpenInterest", 0),
                        "Date": today,
                        "Expiry": expiry
                    })

                # Process Put Option (PE)
                pe_data = rec.get("PE")
                if pe_data:
                    rows.append({
                        "Strike Price": strike,
                        "Option Type": "PE",
                        "Last Price": pe_data.get("lastPrice", 0),
                        "IV": pe_data.get("impliedVolatility", 0),
                        "Open Interest": pe_data.get("openInterest", 0),
                        "Change in OI": pe_data.get("changeinOpenInterest", 0),
                        "Date": today,
                        "Expiry": expiry
                    })

            if not rows:
                self.logger.error("No valid rows created from raw data")
                return None

            # Create DataFrame
            df = pd.DataFrame(rows)

            # Clean numeric columns - handle None, commas, and dashes
            numeric_cols = ["Strike Price", "Last Price", "IV", "Open Interest", "Change in OI"]
            for col in numeric_cols:
                if col in df.columns:
                    # Convert to string first, handle NaN values
                    df[col] = df[col].fillna(0).astype(str)
                    # Remove commas and replace dashes with 0
                    df[col] = df[col].str.replace(",", "", regex=False).replace("-", "0")
                    # Convert to numeric, coercing errors to NaN
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    # Fill NaN values with 0
                    df[col] = df[col].fillna(0)

            # Clean date columns
            df["Date"] = pd.to_datetime(df["Date"])
            df["Expiry"] = pd.to_datetime(df["Expiry"], errors="coerce")

            # Remove rows where strike price is still invalid after cleaning
            df = df[df["Strike Price"] > 0]

            if df.empty:
                self.logger.error("All rows filtered out during cleaning")
                return None

            self.logger.info(f"Successfully cleaned and processed {len(df)} option records")
            return df

        except Exception as e:
            self.logger.error(f"Error cleaning and processing option chain data: {e}")
            return None

    def _validate_processed_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Validate the processed option chain data using existing validation system.
        """
        try:
            from utils.data_validation import clean_and_validate_data

            df_clean, quality_report = clean_and_validate_data(df, "option")

            if not quality_report.passed:
                self.logger.error(f"Option chain data quality validation failed:\n{quality_report.summary()}")
                return None

            if quality_report.warnings:
                self.logger.warning(f"Option chain data quality warnings:\n{quality_report.summary()}")

            self.logger.info("Data validation passed successfully")
            return df_clean

        except Exception as e:
            self.logger.error(f"Error during data validation: {e}")
            return None

    def _save_to_database(self, df: pd.DataFrame, symbol: str = "NIFTY") -> bool:
        """
        Save processed option chain data directly to database using specialized functions.
        """
        try:
            from database.db_manager import save_option_chain_data

            # Use specialized option chain database function
            success = save_option_chain_data(df, symbol=symbol, replace_existing=True)

            if success:
                self.logger.info(f"Successfully saved {len(df)} records to database")
                return True
            else:
                self.logger.error("Database save operation failed")
                return False

        except Exception as e:
            self.logger.error(f"Error saving to database: {e}")
            return False

    def _save_to_csv(self, df: pd.DataFrame, csv_path: str) -> bool:
        """
        Save processed data to CSV file.
        """
        try:
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            df.to_csv(csv_path, index=False)
            self.logger.info(f"Successfully saved processed data to {csv_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error saving to CSV {csv_path}: {e}")
            return False


# Convenience function for backward compatibility and easy usage
def fetch_and_process_option_chain_auto(symbol: str = "NIFTY", **kwargs) -> Optional[pd.DataFrame]:
    """
    Convenience function for automated option chain processing.

    Args:
        symbol: NSE symbol to fetch (default: NIFTY)
        **kwargs: Additional arguments passed to OptionChainProcessor

    Returns:
        Processed DataFrame or None if failed
    """
    processor = OptionChainProcessor()
    return processor.fetch_and_process_option_chain(symbol, **kwargs)


if __name__ == "__main__":
    # Test the automated processor
    print("Testing automated option chain processor...")

    processor = OptionChainProcessor()
    result = processor.fetch_and_process_option_chain(
        symbol="NIFTY",
        save_to_database=True,
        save_to_csv=True
    )

    if result is not None:
        print(f"✅ Successfully processed {len(result)} option chain records")
        print("\nSample data:")
        print(result.head())
    else:
        print("❌ Failed to process option chain data")