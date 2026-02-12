"""
Integration tests for F&O AI Trader data pipeline.
"""
import unittest
import tempfile
import os
import pandas as pd
import sys
from unittest.mock import patch, MagicMock
import sqlite3

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_manager import save_df, load_table, get_conn
from utils.data_validation import clean_and_validate_data


class TestDataPipelineIntegration(unittest.TestCase):
    """Integration tests for the complete data pipeline."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.temp_dir, "test_integration.db")

        # Mock the DB_PATH for integration tests
        self.db_path_patcher = patch('database.db_manager.DB_PATH', self.test_db_path)
        self.db_path_patcher.start()

        # Create sample data
        self.sample_nifty_data = pd.DataFrame({
            'Datetime': pd.date_range('2024-01-01', periods=100, freq='5min'),
            'Open': [20000 + i for i in range(100)],
            'High': [20005 + i for i in range(100)],
            'Low': [19995 + i for i in range(100)],
            'Close': [20002 + i for i in range(100)],
            'Volume': [1000 + (i * 10) for i in range(100)]
        })

        self.sample_option_data = pd.DataFrame({
            'Strike Price': [19500, 19600, 19700, 19800, 19900, 20000] * 10,
            'Option Type': ['CE', 'PE'] * 30,
            'Last Price': [50.0, 45.0, 40.0, 35.0, 30.0, 25.0] * 10,
            'IV': [25.0, 24.0, 23.0, 22.0, 21.0, 20.0] * 10,
            'Open Interest': [1000, 1100, 1200, 1300, 1400, 1500] * 10,
            'Date': pd.date_range('2024-01-01', periods=60, freq='1H'),
            'Expiry': pd.to_datetime('2024-02-01')
        })

    def tearDown(self):
        """Clean up test environment."""
        self.db_path_patcher.stop()
        if os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)

    def test_complete_nifty_data_pipeline(self):
        """Test complete pipeline from raw NIFTY data to processed features."""

        # Step 1: Save raw NIFTY data
        save_df(self.sample_nifty_data, "underlying_raw")

        # Step 2: Load and validate
        loaded_data = load_table("underlying_raw")
        self.assertEqual(len(loaded_data), len(self.sample_nifty_data))

        # Step 3: Clean and validate
        cleaned_data, validation_report = clean_and_validate_data(loaded_data, "price")
        self.assertTrue(validation_report.passed or len(validation_report.errors) == 0)

        # Step 4: Add technical indicators (simplified)
        cleaned_data['ema9'] = cleaned_data['Close'].rolling(9).mean()
        cleaned_data['rsi'] = 50.0  # Simplified RSI
        cleaned_data['atr'] = 10.0  # Simplified ATR
        cleaned_data['vwap'] = cleaned_data['Close']  # Simplified VWAP

        # Step 5: Save processed features
        save_df(cleaned_data, "underlying_features")

        # Step 6: Verify processed data
        processed_data = load_table("underlying_features")
        self.assertIn('ema9', processed_data.columns)
        self.assertIn('rsi', processed_data.columns)
        self.assertIn('atr', processed_data.columns)
        self.assertIn('vwap', processed_data.columns)

    def test_complete_option_data_pipeline(self):
        """Test complete pipeline from raw option data to processed features."""

        # Step 1: Save raw option data
        save_df(self.sample_option_data, "option_raw")

        # Step 2: Load and validate
        loaded_data = load_table("option_raw")
        self.assertEqual(len(loaded_data), len(self.sample_option_data))

        # Step 3: Clean and validate
        cleaned_data, validation_report = clean_and_validate_data(loaded_data, "option")

        # Should pass validation or have only warnings
        if not validation_report.passed:
            self.assertEqual(len(validation_report.errors), 0, f"Errors found: {validation_report.errors}")

        # Step 4: Add option-specific features
        cleaned_data['moneyness'] = cleaned_data['Strike Price'] / 20000  # Simplified moneyness
        cleaned_data['time_to_expiry'] = 30.0  # Simplified time to expiry
        cleaned_data['delta'] = 0.5  # Simplified delta

        # Step 5: Save processed features
        save_df(cleaned_data, "option_chain_features")

        # Step 6: Verify processed data
        processed_data = load_table("option_chain_features")
        self.assertIn('moneyness', processed_data.columns)
        self.assertIn('time_to_expiry', processed_data.columns)
        self.assertIn('delta', processed_data.columns)

    def test_data_quality_throughout_pipeline(self):
        """Test that data quality is maintained throughout the pipeline."""

        # Start with intentionally messy data
        messy_data = self.sample_nifty_data.copy()

        # Add some quality issues
        messy_data.loc[5, 'High'] = messy_data.loc[5, 'Low'] - 1  # Invalid OHLC
        messy_data.loc[10, 'Close'] = None  # Missing value
        messy_data = pd.concat([messy_data, messy_data.iloc[:5]])  # Add duplicates

        # Step 1: Save messy data
        save_df(messy_data, "messy_raw")

        # Step 2: Load and clean
        loaded_data = load_table("messy_raw")
        cleaned_data, validation_report = clean_and_validate_data(loaded_data, "price")

        # Step 3: Verify cleaning worked
        self.assertLessEqual(len(cleaned_data), len(messy_data))  # Should have fewer rows after cleaning

        # Step 4: Check that cleaned data has better quality
        duplicate_count_before = messy_data.duplicated().sum()
        duplicate_count_after = cleaned_data.duplicated().sum()
        self.assertLessEqual(duplicate_count_after, duplicate_count_before)

    def test_database_connection_handling(self):
        """Test that database connections are properly managed."""

        # Test multiple operations to ensure connections are properly closed
        for i in range(10):
            test_data = pd.DataFrame({
                'col1': [f'value_{i}'],
                'col2': [i]
            })
            save_df(test_data, f"test_table_{i}")

        # Check that we can still connect and query
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        conn.close()

        # Should have multiple tables
        self.assertGreaterEqual(len(tables), 10)

    def test_end_to_end_model_data_preparation(self):
        """Test end-to-end data preparation for model training."""

        # Step 1: Prepare underlying data with features
        underlying_data = self.sample_nifty_data.copy()
        underlying_data['ema9'] = underlying_data['Close'].rolling(9).mean()
        underlying_data['ema21'] = underlying_data['Close'].rolling(21).mean()
        underlying_data['rsi'] = 50.0
        underlying_data['atr'] = 10.0
        underlying_data['vwap'] = underlying_data['Close']

        # Step 2: Save to database
        save_df(underlying_data, "underlying_features")

        # Step 3: Load for model training (simulate model training script)
        model_data = load_table("underlying_features")

        # Step 4: Verify model-ready data
        required_features = ['ema9', 'ema21', 'rsi', 'atr', 'vwap']
        for feature in required_features:
            self.assertIn(feature, model_data.columns)

        # Step 5: Create target variable (simplified)
        model_data['target'] = (model_data['Close'].shift(-1) > model_data['Close']).astype(int)
        model_data = model_data.dropna()

        # Step 6: Verify target variable
        self.assertIn('target', model_data.columns)
        self.assertEqual(set(model_data['target'].unique()), {0, 1})

        # Step 7: Verify we have enough data for training
        self.assertGreater(len(model_data), 50)

    def test_concurrent_database_operations(self):
        """Test that concurrent database operations work correctly."""
        import threading

        results = []
        errors = []

        def db_operation(thread_id):
            try:
                test_data = pd.DataFrame({
                    'thread_id': [thread_id] * 5,
                    'value': range(5),
                    'timestamp': pd.date_range('2024-01-01', periods=5, freq='1min')
                })
                save_df(test_data, f"thread_test_{thread_id}")
                loaded_data = load_table(f"thread_test_{thread_id}")
                results.append((thread_id, len(loaded_data)))
            except Exception as e:
                errors.append((thread_id, str(e)))

        # Create multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=db_operation, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check results
        self.assertEqual(len(errors), 0, f"Database errors in threads: {errors}")
        self.assertEqual(len(results), 5, "Not all threads completed successfully")

        # Verify all operations completed
        for thread_id, row_count in results:
            self.assertEqual(row_count, 5, f"Thread {thread_id} didn't save/load correctly")


class TestAPIDataIntegration(unittest.TestCase):
    """Integration tests for API data processing."""

    def test_mock_nse_api_integration(self):
        """Test integration with mocked NSE API data."""

        # Mock NSE API response
        mock_nse_response = {
            "records": {
                "data": [
                    {
                        "strikePrice": 20000,
                        "expiryDate": "01-Feb-2024",
                        "CE": {
                            "lastPrice": 50.0,
                            "impliedVolatility": 25.0,
                            "openInterest": 1000,
                            "changeinOpenInterest": 100
                        },
                        "PE": {
                            "lastPrice": 45.0,
                            "impliedVolatility": 24.0,
                            "openInterest": 1100,
                            "changeinOpenInterest": -50
                        }
                    }
                ]
            }
        }

        # Simulate processing NSE data
        records = mock_nse_response.get("records", {}).get("data", [])
        rows = []

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
                        "Date": pd.Timestamp.now(),
                        "Expiry": expiry
                    })

        df = pd.DataFrame(rows)

        # Validate the processed data
        cleaned_data, validation_report = clean_and_validate_data(df, "option")

        # Should pass validation
        if not validation_report.passed:
            self.assertEqual(len(validation_report.errors), 0, f"Validation errors: {validation_report.errors}")

        # Verify data structure
        self.assertIn("Strike Price", cleaned_data.columns)
        self.assertIn("Option Type", cleaned_data.columns)
        self.assertEqual(len(cleaned_data), 2)  # CE and PE
        self.assertEqual(set(cleaned_data["Option Type"]), {"CE", "PE"})


if __name__ == '__main__':
    unittest.main()