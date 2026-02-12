"""
Unit tests for database manager.
"""
import unittest
import tempfile
import os
import pandas as pd
import sys
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_manager import (
    validate_table_name,
    save_df,
    load_table,
    get_conn,
    ALLOWED_TABLES
)


class TestDatabaseManager(unittest.TestCase):
    """Test cases for database manager."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.temp_dir, "test.db")

        # Mock the DB_PATH
        self.db_path_patcher = patch('database.db_manager.DB_PATH', self.test_db_path)
        self.db_path_patcher.start()

    def tearDown(self):
        """Clean up test environment."""
        self.db_path_patcher.stop()
        # Clean up temp directory
        if os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)

    def test_validate_table_name_valid(self):
        """Test table name validation with valid names."""
        valid_names = [
            "underlying_features",
            "option_chain_features",
            "test_table_123",
            "_private_table"
        ]

        for name in valid_names:
            try:
                result = validate_table_name(name)
                self.assertEqual(result, name)
            except Exception as e:
                self.fail(f"Valid table name '{name}' should not raise exception: {e}")

    def test_validate_table_name_invalid(self):
        """Test table name validation with invalid names."""
        invalid_names = [
            "table with spaces",
            "table-with-dashes",
            "table.with.dots",
            "123_starts_with_number",
            "",
            None,
            "table; DROP TABLE users;--"  # SQL injection attempt
        ]

        for name in invalid_names:
            with self.assertRaises(ValueError, msg=f"Invalid table name '{name}' should raise ValueError"):
                validate_table_name(name)

    def test_save_and_load_dataframe(self):
        """Test saving and loading DataFrames."""
        # Create test DataFrame
        test_data = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Price': [100.0, 101.5, 99.8, 102.1, 103.0],
            'Volume': [1000, 1200, 800, 1500, 1100]
        })

        table_name = "underlying_features"

        # Test saving
        try:
            save_df(test_data, table_name)
        except Exception as e:
            self.fail(f"Saving DataFrame should not raise exception: {e}")

        # Test loading
        try:
            loaded_data = load_table(table_name)
            self.assertIsInstance(loaded_data, pd.DataFrame)
            self.assertEqual(len(loaded_data), len(test_data))
            self.assertEqual(list(loaded_data.columns), list(test_data.columns))
        except Exception as e:
            self.fail(f"Loading DataFrame should not raise exception: {e}")

    def test_save_empty_dataframe(self):
        """Test handling of empty DataFrames."""
        empty_df = pd.DataFrame()

        # Should handle empty DataFrame gracefully
        with patch('database.db_manager.logger') as mock_logger:
            save_df(empty_df, "underlying_features")
            mock_logger.warning.assert_called_once_with("Attempted to save empty dataframe")

    def test_save_none_dataframe(self):
        """Test handling of None DataFrame."""
        with patch('database.db_manager.logger') as mock_logger:
            save_df(None, "underlying_features")
            mock_logger.warning.assert_called_once_with("Attempted to save empty dataframe")

    def test_get_connection(self):
        """Test database connection."""
        try:
            conn = get_conn()
            self.assertIsNotNone(conn)
            conn.close()
        except Exception as e:
            self.fail(f"Getting database connection should not raise exception: {e}")

    def test_load_nonexistent_table(self):
        """Test loading from nonexistent table."""
        with self.assertRaises(Exception):
            load_table("nonexistent_table")


class TestDatabaseSQLInjection(unittest.TestCase):
    """Test SQL injection prevention."""

    def test_sql_injection_prevention(self):
        """Test that SQL injection attempts are blocked."""
        malicious_table_names = [
            "users; DROP TABLE users;--",
            "test' OR '1'='1",
            "'; DELETE FROM users; --",
            "UNION SELECT * FROM sensitive_data"
        ]

        for malicious_name in malicious_table_names:
            with self.assertRaises(ValueError, msg=f"SQL injection attempt '{malicious_name}' should be blocked"):
                validate_table_name(malicious_name)


if __name__ == '__main__':
    unittest.main()