"""
Data validation and quality checks for F&O AI Trader.
"""
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional, Any
from config.logging_config import get_logger

logger = get_logger('data_validation')


class DataValidationError(Exception):
    """Custom exception for data validation errors."""
    pass


class DataQualityReport:
    """Class to hold data quality assessment results."""

    def __init__(self):
        self.passed = True
        self.warnings = []
        self.errors = []
        self.metrics = {}

    def add_warning(self, message: str):
        """Add a warning to the report."""
        self.warnings.append(message)
        logger.warning(f"Data Quality Warning: {message}")

    def add_error(self, message: str):
        """Add an error to the report."""
        self.errors.append(message)
        self.passed = False
        logger.error(f"Data Quality Error: {message}")

    def add_metric(self, name: str, value: Any):
        """Add a quality metric."""
        self.metrics[name] = value

    def summary(self) -> str:
        """Get a summary of the data quality report."""
        status = "PASSED" if self.passed else "FAILED"
        summary = f"Data Quality Report: {status}\n"
        summary += f"Warnings: {len(self.warnings)}\n"
        summary += f"Errors: {len(self.errors)}\n"

        if self.warnings:
            summary += "\nWarnings:\n" + "\n".join(f"  - {w}" for w in self.warnings)

        if self.errors:
            summary += "\nErrors:\n" + "\n".join(f"  - {e}" for e in self.errors)

        if self.metrics:
            summary += "\nMetrics:\n" + "\n".join(f"  {k}: {v}" for k, v in self.metrics.items())

        return summary


def validate_api_response(data: Dict, expected_keys: List[str], response_name: str = "API Response") -> DataQualityReport:
    """
    Validate API response structure and content.

    Args:
        data: API response data
        expected_keys: List of expected keys in the response
        response_name: Name of the API response for logging

    Returns:
        DataQualityReport object
    """
    report = DataQualityReport()

    if not isinstance(data, dict):
        report.add_error(f"{response_name} is not a dictionary")
        return report

    # Check for expected keys
    missing_keys = [key for key in expected_keys if key not in data]
    if missing_keys:
        report.add_error(f"{response_name} missing required keys: {missing_keys}")

    # Check for empty response
    if not data:
        report.add_error(f"{response_name} is empty")
    else:
        report.add_metric("response_keys", list(data.keys()))
        report.add_metric("response_size", len(data))

    return report


def validate_nse_option_chain_data(data: Dict) -> DataQualityReport:
    """Validate NSE option chain API response."""
    report = DataQualityReport()

    # Check basic structure
    basic_validation = validate_api_response(data, ["records"], "NSE Option Chain")
    report.warnings.extend(basic_validation.warnings)
    report.errors.extend(basic_validation.errors)

    if not basic_validation.passed:
        return report

    records = data.get("records", {})
    if not isinstance(records, dict):
        report.add_error("NSE records is not a dictionary")
        return report

    # Check for data array
    option_data = records.get("data", [])
    if not isinstance(option_data, list):
        report.add_error("NSE option data is not a list")
        return report

    if len(option_data) == 0:
        report.add_error("NSE option data is empty")
        return report

    report.add_metric("total_strikes", len(option_data))

    # Validate individual option records
    valid_records = 0
    for i, record in enumerate(option_data[:10]):  # Sample first 10 records
        if validate_option_record(record, f"Record {i}"):
            valid_records += 1

    if valid_records == 0:
        report.add_error("No valid option records found in sample")
    elif valid_records < 5:
        report.add_warning(f"Only {valid_records}/10 sample records are valid")

    return report


def validate_option_record(record: Dict, record_name: str) -> bool:
    """Validate individual option record structure."""
    required_fields = ["strikePrice", "CE", "PE"]
    for field in required_fields:
        if field not in record:
            logger.warning(f"{record_name}: Missing field {field}")
            return False

    # Validate strike price
    strike = record.get("strikePrice")
    if not isinstance(strike, (int, float)) or strike <= 0:
        logger.warning(f"{record_name}: Invalid strike price {strike}")
        return False

    return True


def validate_dataframe_structure(df: pd.DataFrame, required_columns: List[str], df_name: str = "DataFrame") -> DataQualityReport:
    """
    Validate DataFrame structure and basic quality.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        df_name: Name of DataFrame for logging

    Returns:
        DataQualityReport object
    """
    report = DataQualityReport()

    if df is None:
        report.add_error(f"{df_name} is None")
        return report

    if df.empty:
        report.add_error(f"{df_name} is empty")
        return report

    report.add_metric("total_rows", len(df))
    report.add_metric("total_columns", len(df.columns))

    # Check for required columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        report.add_error(f"{df_name} missing required columns: {missing_columns}")

    # Check for completely empty columns
    empty_columns = [col for col in df.columns if df[col].isna().all()]
    if empty_columns:
        report.add_warning(f"{df_name} has completely empty columns: {empty_columns}")

    # Check for high missing data percentage
    for col in df.columns:
        if col in required_columns:
            missing_pct = (df[col].isna().sum() / len(df)) * 100
            report.add_metric(f"{col}_missing_pct", round(missing_pct, 2))

            if missing_pct > 50:
                report.add_error(f"{df_name} column '{col}' has {missing_pct:.1f}% missing values")
            elif missing_pct > 20:
                report.add_warning(f"{df_name} column '{col}' has {missing_pct:.1f}% missing values")

    return report


def validate_price_data(df: pd.DataFrame, df_name: str = "Price Data") -> DataQualityReport:
    """Validate OHLC price data quality."""
    report = DataQualityReport()

    required_columns = ["Open", "High", "Low", "Close"]
    structure_validation = validate_dataframe_structure(df, required_columns, df_name)
    report.warnings.extend(structure_validation.warnings)
    report.errors.extend(structure_validation.errors)

    if not structure_validation.passed:
        return report

    # Validate OHLC relationships
    invalid_ohlc = 0
    for idx, row in df.iterrows():
        try:
            o, h, l, c = row["Open"], row["High"], row["Low"], row["Close"]

            # Skip if any values are NaN
            if pd.isna([o, h, l, c]).any():
                continue

            # High should be >= Open, Close, Low
            # Low should be <= Open, Close, High
            if not (h >= max(o, c, l) and l <= min(o, c, h)):
                invalid_ohlc += 1

        except (KeyError, TypeError):
            continue

    if invalid_ohlc > 0:
        invalid_pct = (invalid_ohlc / len(df)) * 100
        report.add_metric("invalid_ohlc_pct", round(invalid_pct, 2))

        if invalid_pct > 5:
            report.add_error(f"{df_name}: {invalid_pct:.1f}% of rows have invalid OHLC relationships")
        elif invalid_pct > 1:
            report.add_warning(f"{df_name}: {invalid_pct:.1f}% of rows have invalid OHLC relationships")

    # Check for extreme price movements (potential data errors)
    if "Close" in df.columns and len(df) > 1:
        df_sorted = df.sort_values("Datetime") if "Datetime" in df.columns else df
        price_changes = df_sorted["Close"].pct_change()
        extreme_moves = abs(price_changes) > 0.1  # 10% moves

        if extreme_moves.sum() > 0:
            extreme_pct = (extreme_moves.sum() / len(df)) * 100
            report.add_metric("extreme_moves_pct", round(extreme_pct, 2))

            if extreme_pct > 2:
                report.add_warning(f"{df_name}: {extreme_pct:.1f}% of price movements are >10%")

    return report


def validate_option_data(df: pd.DataFrame, df_name: str = "Option Data") -> DataQualityReport:
    """Validate options data quality."""
    report = DataQualityReport()

    required_columns = ["Strike Price", "Option Type", "Last Price"]
    structure_validation = validate_dataframe_structure(df, required_columns, df_name)
    report.warnings.extend(structure_validation.warnings)
    report.errors.extend(structure_validation.errors)

    if not structure_validation.passed:
        return report

    # Validate option types
    if "Option Type" in df.columns:
        valid_types = {"CE", "PE", "CALL", "PUT"}
        invalid_types = df[~df["Option Type"].isin(valid_types)]["Option Type"].unique()

        if len(invalid_types) > 0:
            report.add_error(f"{df_name}: Invalid option types found: {invalid_types}")

        # Check CE/PE balance
        type_counts = df["Option Type"].value_counts()
        report.add_metric("option_type_distribution", type_counts.to_dict())

    # Validate strike prices
    if "Strike Price" in df.columns:
        # Convert to numeric for validation, handling string values
        strike_prices = pd.to_numeric(df["Strike Price"], errors="coerce")
        negative_strikes = (strike_prices <= 0).sum()
        if negative_strikes > 0:
            report.add_error(f"{df_name}: {negative_strikes} rows have invalid strike prices (<=0)")

    # Validate option prices
    if "Last Price" in df.columns:
        # Convert to numeric for validation, handling string values
        last_prices = pd.to_numeric(df["Last Price"], errors="coerce")
        negative_prices = (last_prices < 0).sum()
        if negative_prices > 0:
            report.add_error(f"{df_name}: {negative_prices} rows have negative option prices")

    return report


def validate_expiry_dates(df: pd.DataFrame, current_date: Optional[date] = None) -> DataQualityReport:
    """Validate expiry dates are reasonable and not hardcoded."""
    report = DataQualityReport()

    if current_date is None:
        current_date = date.today()

    if "Expiry" not in df.columns:
        report.add_warning("No Expiry column found for validation")
        return report

    # Check for hardcoded dates (suspiciously common dates)
    expiry_counts = df["Expiry"].value_counts()
    most_common_expiry = expiry_counts.index[0] if len(expiry_counts) > 0 else None

    if most_common_expiry and expiry_counts.iloc[0] > len(df) * 0.8:
        report.add_warning(f"Most common expiry date ({most_common_expiry}) appears in {expiry_counts.iloc[0]}/{len(df)} rows - possibly hardcoded")

    # Check for past expiries
    df_expiry = df["Expiry"].dropna()
    if len(df_expiry) > 0:
        past_expiries = pd.to_datetime(df_expiry) < pd.Timestamp(current_date)
        past_count = past_expiries.sum()

        if past_count > 0:
            report.add_warning(f"{past_count} options have expiry dates in the past")

    return report


def clean_and_validate_data(df: pd.DataFrame, data_type: str = "generic") -> Tuple[pd.DataFrame, DataQualityReport]:
    """
    Clean and validate data with comprehensive quality checks.

    Args:
        df: Input DataFrame
        data_type: Type of data ('price', 'option', 'generic')

    Returns:
        Tuple of (cleaned_dataframe, quality_report)
    """
    report = DataQualityReport()
    df_clean = df.copy()

    logger.info(f"Starting data cleaning and validation for {data_type} data")

    # Basic validation
    if data_type == "price":
        validation_result = validate_price_data(df_clean, f"{data_type.title()} Data")
    elif data_type == "option":
        validation_result = validate_option_data(df_clean, f"{data_type.title()} Data")
        expiry_validation = validate_expiry_dates(df_clean)
        validation_result.warnings.extend(expiry_validation.warnings)
        validation_result.errors.extend(expiry_validation.errors)
    else:
        validation_result = validate_dataframe_structure(df_clean, [], f"{data_type.title()} Data")

    report.warnings.extend(validation_result.warnings)
    report.errors.extend(validation_result.errors)
    report.metrics.update(validation_result.metrics)

    # Basic cleaning
    initial_rows = len(df_clean)

    # Remove completely duplicate rows
    df_clean = df_clean.drop_duplicates()
    duplicates_removed = initial_rows - len(df_clean)
    if duplicates_removed > 0:
        report.add_warning(f"Removed {duplicates_removed} duplicate rows")

    # Convert numeric columns
    numeric_columns = ["Open", "High", "Low", "Close", "Volume", "Last Price", "Strike Price", "IV"]
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

    # Convert datetime columns
    datetime_columns = ["Datetime", "Date", "Expiry"]
    for col in datetime_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors="coerce")

    final_rows = len(df_clean)
    report.add_metric("rows_initial", initial_rows)
    report.add_metric("rows_final", final_rows)
    report.add_metric("rows_cleaned", initial_rows - final_rows)

    logger.info(f"Data cleaning completed: {initial_rows} -> {final_rows} rows")

    return df_clean, report