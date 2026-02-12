"""
Unit tests for data validation module.
"""
import unittest
import pandas as pd
import numpy as np
import sys
import os
from datetime import date, datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.data_validation import (
    validate_api_response,
    validate_nse_option_chain_data,
    validate_dataframe_structure,
    validate_price_data,
    validate_option_data,
    validate_expiry_dates,
    clean_and_validate_data,
    DataQualityReport
)


class TestDataQualityReport(unittest.TestCase):
    """Test DataQualityReport class."""

    def test_report_initialization(self):
        """Test report initialization."""
        report = DataQualityReport()
        self.assertTrue(report.passed)
        self.assertEqual(len(report.warnings), 0)
        self.assertEqual(len(report.errors), 0)
        self.assertEqual(len(report.metrics), 0)

    def test_add_warning(self):
        """Test adding warnings."""
        report = DataQualityReport()
        report.add_warning("Test warning")
        self.assertTrue(report.passed)  # Warnings don't fail the report
        self.assertEqual(len(report.warnings), 1)
        self.assertEqual(report.warnings[0], "Test warning")

    def test_add_error(self):
        """Test adding errors."""
        report = DataQualityReport()
        report.add_error("Test error")
        self.assertFalse(report.passed)  # Errors fail the report
        self.assertEqual(len(report.errors), 1)
        self.assertEqual(report.errors[0], "Test error")

    def test_add_metric(self):
        """Test adding metrics."""
        report = DataQualityReport()
        report.add_metric("test_metric", 42)
        self.assertEqual(report.metrics["test_metric"], 42)


class TestAPIResponseValidation(unittest.TestCase):
    """Test API response validation."""

    def test_valid_api_response(self):
        """Test validation of valid API response."""
        valid_response = {
            "status": "success",
            "data": [{"key": "value"}],
            "timestamp": "2024-01-01T00:00:00Z"
        }

        report = validate_api_response(valid_response, ["status", "data"], "Test API")
        self.assertTrue(report.passed)
        self.assertEqual(len(report.errors), 0)

    def test_missing_keys_response(self):
        """Test validation with missing required keys."""
        invalid_response = {
            "status": "success"
            # Missing "data" key
        }

        report = validate_api_response(invalid_response, ["status", "data"], "Test API")
        self.assertFalse(report.passed)
        self.assertGreater(len(report.errors), 0)

    def test_empty_response(self):
        """Test validation of empty response."""
        report = validate_api_response({}, ["key"], "Test API")
        self.assertFalse(report.passed)

    def test_non_dict_response(self):
        """Test validation of non-dictionary response."""
        report = validate_api_response("not a dict", ["key"], "Test API")
        self.assertFalse(report.passed)


class TestDataFrameValidation(unittest.TestCase):
    """Test DataFrame validation functions."""

    def setUp(self):
        """Set up test data."""
        self.valid_price_data = pd.DataFrame({
            'Datetime': pd.date_range('2024-01-01', periods=5, freq='1H'),
            'Open': [100.0, 101.0, 102.0, 103.0, 104.0],
            'High': [101.0, 102.0, 103.0, 104.0, 105.0],
            'Low': [99.0, 100.0, 101.0, 102.0, 103.0],
            'Close': [100.5, 101.5, 102.5, 103.5, 104.5],
            'Volume': [1000, 1100, 1200, 1300, 1400]
        })

        self.valid_option_data = pd.DataFrame({
            'Strike Price': [20000, 20100, 20200, 20300, 20400],
            'Option Type': ['CE', 'PE', 'CE', 'PE', 'CE'],
            'Last Price': [50.0, 45.0, 40.0, 35.0, 30.0],
            'IV': [25.0, 24.0, 23.0, 22.0, 21.0],
            'Expiry': pd.date_range('2024-02-01', periods=5)
        })

    def test_valid_dataframe_structure(self):
        """Test validation of valid DataFrame structure."""
        required_cols = ['Open', 'High', 'Low', 'Close']
        report = validate_dataframe_structure(self.valid_price_data, required_cols, "Test Data")
        self.assertTrue(report.passed)

    def test_missing_columns(self):
        """Test detection of missing required columns."""
        df_missing_cols = self.valid_price_data.drop(columns=['High', 'Low'])
        required_cols = ['Open', 'High', 'Low', 'Close']
        report = validate_dataframe_structure(df_missing_cols, required_cols, "Test Data")
        self.assertFalse(report.passed)

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        empty_df = pd.DataFrame()
        report = validate_dataframe_structure(empty_df, ['col1'], "Test Data")
        self.assertFalse(report.passed)

    def test_none_dataframe(self):
        """Test handling of None DataFrame."""
        report = validate_dataframe_structure(None, ['col1'], "Test Data")
        self.assertFalse(report.passed)


class TestPriceDataValidation(unittest.TestCase):
    """Test price data specific validation."""

    def setUp(self):
        """Set up test data."""
        self.valid_price_data = pd.DataFrame({
            'Datetime': pd.date_range('2024-01-01', periods=5, freq='1H'),
            'Open': [100.0, 101.0, 102.0, 103.0, 104.0],
            'High': [101.0, 102.0, 103.0, 104.0, 105.0],
            'Low': [99.0, 100.0, 101.0, 102.0, 103.0],
            'Close': [100.5, 101.5, 102.5, 103.5, 104.5],
            'Volume': [1000, 1100, 1200, 1300, 1400]
        })

    def test_valid_price_data(self):
        """Test validation of valid price data."""
        report = validate_price_data(self.valid_price_data, "Test Price Data")
        self.assertTrue(report.passed)

    def test_invalid_ohlc_relationships(self):
        """Test detection of invalid OHLC relationships."""
        invalid_ohlc_data = self.valid_price_data.copy()
        # Make High lower than Close (invalid)
        invalid_ohlc_data.loc[0, 'High'] = 90.0  # Lower than Open, Close, Low
        invalid_ohlc_data.loc[0, 'Close'] = 100.5

        report = validate_price_data(invalid_ohlc_data, "Test Price Data")
        # Should detect invalid OHLC but might still pass overall
        self.assertIn('invalid_ohlc_pct', report.metrics)

    def test_extreme_price_movements(self):
        """Test detection of extreme price movements."""
        extreme_data = self.valid_price_data.copy()
        # Create a 50% price jump
        extreme_data.loc[1, 'Close'] = 150.0

        report = validate_price_data(extreme_data, "Test Price Data")
        self.assertIn('extreme_moves_pct', report.metrics)


class TestOptionDataValidation(unittest.TestCase):
    """Test option data specific validation."""

    def setUp(self):
        """Set up test data."""
        self.valid_option_data = pd.DataFrame({
            'Strike Price': [20000, 20100, 20200],
            'Option Type': ['CE', 'PE', 'CE'],
            'Last Price': [50.0, 45.0, 40.0],
            'IV': [25.0, 24.0, 23.0]
        })

    def test_valid_option_data(self):
        """Test validation of valid option data."""
        report = validate_option_data(self.valid_option_data, "Test Option Data")
        self.assertTrue(report.passed)

    def test_invalid_option_types(self):
        """Test detection of invalid option types."""
        invalid_data = self.valid_option_data.copy()
        invalid_data.loc[0, 'Option Type'] = 'INVALID'

        report = validate_option_data(invalid_data, "Test Option Data")
        self.assertFalse(report.passed)

    def test_negative_strike_prices(self):
        """Test detection of invalid strike prices."""
        invalid_data = self.valid_option_data.copy()
        invalid_data.loc[0, 'Strike Price'] = -1000

        report = validate_option_data(invalid_data, "Test Option Data")
        self.assertFalse(report.passed)

    def test_negative_option_prices(self):
        """Test detection of negative option prices."""
        invalid_data = self.valid_option_data.copy()
        invalid_data.loc[0, 'Last Price'] = -10.0

        report = validate_option_data(invalid_data, "Test Option Data")
        self.assertFalse(report.passed)


class TestExpiryDateValidation(unittest.TestCase):
    """Test expiry date validation."""

    def setUp(self):
        """Set up test data."""
        future_date = date.today() + timedelta(days=30)
        past_date = date.today() - timedelta(days=1)

        self.valid_expiry_data = pd.DataFrame({
            'Expiry': [future_date, future_date, future_date],
            'Strike Price': [20000, 20100, 20200]
        })

        self.past_expiry_data = pd.DataFrame({
            'Expiry': [past_date, past_date, future_date],
            'Strike Price': [20000, 20100, 20200]
        })

    def test_valid_expiry_dates(self):
        """Test validation with valid future expiry dates."""
        report = validate_expiry_dates(self.valid_expiry_data)
        self.assertTrue(report.passed)

    def test_past_expiry_dates(self):
        """Test detection of past expiry dates."""
        report = validate_expiry_dates(self.past_expiry_data)
        # Should have warnings about past expiries
        self.assertGreater(len(report.warnings), 0)

    def test_missing_expiry_column(self):
        """Test handling of missing expiry column."""
        df_no_expiry = pd.DataFrame({'Strike Price': [20000, 20100]})
        report = validate_expiry_dates(df_no_expiry)
        self.assertGreater(len(report.warnings), 0)


class TestDataCleaning(unittest.TestCase):
    """Test data cleaning functionality."""

    def test_clean_price_data(self):
        """Test cleaning of price data."""
        dirty_data = pd.DataFrame({
            'Datetime': ['2024-01-01', '2024-01-02', '2024-01-01'],  # Duplicate
            'Open': ['100.0', '101.0', '100.0'],  # String numbers
            'High': [101.0, 102.0, 101.0],
            'Low': [99.0, 100.0, 99.0],
            'Close': [100.5, 101.5, 100.5]
        })

        clean_data, report = clean_and_validate_data(dirty_data, "price")

        # Should remove duplicates
        self.assertLess(len(clean_data), len(dirty_data))

        # Should convert strings to numeric
        self.assertTrue(pd.api.types.is_numeric_dtype(clean_data['Open']))

    def test_clean_option_data(self):
        """Test cleaning of option data."""
        dirty_option_data = pd.DataFrame({
            'Strike Price': ['20000', '20100', '20000'],  # String and duplicate
            'Option Type': ['CE', 'PE', 'CE'],
            'Last Price': [50.0, 45.0, 50.0],
            'Expiry': ['2024-02-01', '2024-02-01', '2024-02-01']
        })

        clean_data, report = clean_and_validate_data(dirty_option_data, "option")

        # Should convert strings to appropriate types
        self.assertTrue(pd.api.types.is_numeric_dtype(clean_data['Strike Price']))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(clean_data['Expiry']))


if __name__ == '__main__':
    unittest.main()