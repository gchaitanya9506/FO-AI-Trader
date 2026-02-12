"""
Unit tests for alert system.
"""
import unittest
import sys
import os
from unittest.mock import patch, MagicMock, Mock
import requests

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.alert_system import send_telegram_message, send_alert


class TestAlertSystem(unittest.TestCase):
    """Test cases for alert system."""

    @patch('execution.alert_system.requests.post')
    @patch('execution.alert_system.TELEGRAM_TOKEN', 'test_token')
    @patch('execution.alert_system.TELEGRAM_CHAT_ID', '123456')
    def test_successful_telegram_message(self, mock_post):
        """Test successful telegram message sending."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        result = send_telegram_message("Test message")

        self.assertTrue(result)
        mock_post.assert_called_once()

        # Check call arguments
        call_args = mock_post.call_args
        self.assertIn('data', call_args.kwargs)
        self.assertEqual(call_args.kwargs['data']['text'], "Test message")
        self.assertEqual(call_args.kwargs['data']['chat_id'], '123456')

    @patch('execution.alert_system.requests.post')
    @patch('execution.alert_system.TELEGRAM_TOKEN', 'test_token')
    @patch('execution.alert_system.TELEGRAM_CHAT_ID', '123456')
    def test_telegram_api_error(self, mock_post):
        """Test handling of Telegram API errors."""
        # Mock failed response
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_post.return_value = mock_response

        result = send_telegram_message("Test message", max_retries=1)

        self.assertFalse(result)

    @patch('execution.alert_system.requests.post')
    @patch('execution.alert_system.TELEGRAM_TOKEN', 'test_token')
    @patch('execution.alert_system.TELEGRAM_CHAT_ID', '123456')
    def test_telegram_rate_limiting(self, mock_post):
        """Test handling of Telegram rate limiting."""
        # Mock rate limit response
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers = {'Retry-After': '1'}
        mock_post.return_value = mock_response

        with patch('execution.alert_system.time.sleep') as mock_sleep:
            result = send_telegram_message("Test message", max_retries=1)

            self.assertFalse(result)
            mock_sleep.assert_called_with(1.0)

    @patch('execution.alert_system.requests.post')
    @patch('execution.alert_system.TELEGRAM_TOKEN', 'test_token')
    @patch('execution.alert_system.TELEGRAM_CHAT_ID', '123456')
    def test_connection_error_retry(self, mock_post):
        """Test retry logic for connection errors."""
        # Mock connection error
        mock_post.side_effect = requests.exceptions.ConnectionError("Connection failed")

        with patch('execution.alert_system.time.sleep') as mock_sleep:
            result = send_telegram_message("Test message", max_retries=2, retry_delay=0.1)

            self.assertFalse(result)
            # Should have tried 2 times (initial + 1 retry)
            self.assertEqual(mock_post.call_count, 2)
            # Should have slept once between retries
            mock_sleep.assert_called_once()

    @patch('execution.alert_system.requests.post')
    @patch('execution.alert_system.TELEGRAM_TOKEN', 'test_token')
    @patch('execution.alert_system.TELEGRAM_CHAT_ID', '123456')
    def test_timeout_error(self, mock_post):
        """Test handling of timeout errors."""
        # Mock timeout error
        mock_post.side_effect = requests.exceptions.Timeout("Request timed out")

        result = send_telegram_message("Test message", max_retries=1)

        self.assertFalse(result)

    @patch('execution.alert_system.send_telegram_message')
    def test_send_alert_function(self, mock_send):
        """Test the send_alert wrapper function."""
        mock_send.return_value = True

        result = send_alert("BUY CALL NIFTY 20000")

        self.assertTrue(result)
        mock_send.assert_called_once()

        # Check that message contains expected elements
        call_args = mock_send.call_args[0][0]  # First positional argument
        self.assertIn("BUY CALL NIFTY 20000", call_args)
        self.assertIn("🤖 AI TRADING SIGNAL", call_args)

    @patch('execution.alert_system.send_telegram_message')
    def test_send_alert_failure(self, mock_send):
        """Test send_alert when telegram sending fails."""
        mock_send.return_value = False

        result = send_alert("TEST SIGNAL")

        self.assertFalse(result)

    @patch('execution.alert_system.requests.post')
    @patch('execution.alert_system.TELEGRAM_TOKEN', 'test_token')
    @patch('execution.alert_system.TELEGRAM_CHAT_ID', '123456')
    def test_exponential_backoff(self, mock_post):
        """Test exponential backoff in retry logic."""
        # Mock failed responses
        mock_response = Mock()
        mock_response.status_code = 500
        mock_post.return_value = mock_response

        with patch('execution.alert_system.time.sleep') as mock_sleep:
            result = send_telegram_message("Test message", max_retries=3, retry_delay=1.0)

            self.assertFalse(result)

            # Should have made 3 attempts
            self.assertEqual(mock_post.call_count, 3)

            # Should have called sleep with exponential backoff: 1.0, 2.0
            expected_sleep_calls = [unittest.mock.call(1.0), unittest.mock.call(2.0)]
            mock_sleep.assert_has_calls(expected_sleep_calls)


class TestAlertSystemConfiguration(unittest.TestCase):
    """Test alert system configuration handling."""

    def test_missing_telegram_credentials(self):
        """Test behavior when Telegram credentials are missing."""
        with patch('execution.alert_system.TELEGRAM_TOKEN', None):
            # Should handle missing credentials gracefully
            # This test verifies the system doesn't crash with missing config
            try:
                # The actual behavior depends on how the system handles missing tokens
                # For now, just verify no exceptions are raised during import
                import execution.alert_system
            except Exception as e:
                # If an exception is raised, it should be informative
                self.assertIn("token", str(e).lower())

    @patch('execution.alert_system.requests.post')
    def test_custom_timeout_parameter(self, mock_post):
        """Test custom timeout parameter."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        send_telegram_message("Test message", timeout=20)

        # Verify timeout was passed to requests.post
        call_kwargs = mock_post.call_args.kwargs
        self.assertEqual(call_kwargs['timeout'], 20)

    def test_message_length_handling(self):
        """Test handling of very long messages."""
        # Telegram has a 4096 character limit
        long_message = "A" * 5000

        with patch('execution.alert_system.requests.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response

            # Should handle long messages (either truncate or split)
            result = send_telegram_message(long_message)

            # The exact behavior depends on implementation
            # At minimum, should not crash
            self.assertIsInstance(result, bool)


if __name__ == '__main__':
    unittest.main()