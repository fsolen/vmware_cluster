import unittest
from unittest.mock import patch, MagicMock, call # Added 'call' for checking logger calls
import ssl # Needed for ssl._create_unverified_context

# Assuming modules are accessible in the path
from modules.connection_manager import ConnectionManager
# We will be mocking SmartConnect and Disconnect where they are used in the ConnectionManager module

class TestConnectionManager(unittest.TestCase):

    @patch('modules.connection_manager.ssl._create_unverified_context')
    @patch('modules.connection_manager.connect.Disconnect') # Corrected patch path
    @patch('modules.connection_manager.connect.SmartConnect')
    @patch('modules.connection_manager.logger')
    def test_successful_connection_logs_message(self, mock_logger, mock_smart_connect, mock_disconnect, mock_ssl_context):
        # Configure mocks
        mock_service_instance_return = MagicMock()
        mock_smart_connect.return_value = mock_service_instance_return

        mock_ssl_unverified_context_return = MagicMock()
        mock_ssl_context.return_value = mock_ssl_unverified_context_return

        # Instantiate ConnectionManager
        cm = ConnectionManager("test_vcenter", "test_user", "test_pass")

        # Call connect
        service_instance = cm.connect()

        # Assert logger calls
        # Check for the specific success message
        expected_success_log = call("Successfully connected to vCenter!")
        self.assertIn(expected_success_log, mock_logger.info.call_args_list)

        # Check for the initial connection attempt log
        expected_connecting_log = call("Connecting to vCenter test_vcenter...")
        self.assertIn(expected_connecting_log, mock_logger.info.call_args_list)

        # Assert SmartConnect call
        mock_smart_connect.assert_called_once_with(
            host="test_vcenter",
            user="test_user",
            pwd="test_pass",
            port=443,
            sslContext=mock_ssl_unverified_context_return # Assert that the context object was passed
        )
        mock_ssl_context.assert_called_once() # Ensure _create_unverified_context was called

        # Ensure service_instance is returned
        self.assertIsNotNone(service_instance)
        self.assertEqual(service_instance, mock_service_instance_return)

        # Ensure Disconnect is not called during a successful connect
        mock_disconnect.assert_not_called()

if __name__ == '__main__':
    unittest.main()
