import unittest
from unittest.mock import MagicMock, patch, call
import logging

# Assuming modules are accessible
from modules.resource_monitor import ResourceMonitor
from pyVmomi import vim # For creating specific vim mock objects if necessary

# Configure a logger for tests or patch the one in the module
logger = logging.getLogger('fdrs') 
# logger.setLevel(logging.CRITICAL + 1) # Silence fdrs logs during tests


class TestResourceMonitor(unittest.TestCase):

    def setUp(self):
        # Mock service_instance for ResourceMonitor initialization
        self.mock_si = MagicMock()
        
        # Mock content.perfManager
        self.mock_perf_manager = MagicMock(spec=vim.PerformanceManager)
        self.mock_si.content.perfManager = self.mock_perf_manager
        
        # Mock perfManager.perfCounter for _build_counter_map
        # This needs to be a list of objects with groupInfo.key and nameInfo.key
        mock_counter_cpu = MagicMock()
        mock_counter_cpu.groupInfo.key = "cpu"
        mock_counter_cpu.nameInfo.key = "usage"
        mock_counter_cpu.key = 1 # Actual counter key
        
        mock_counter_mem = MagicMock()
        mock_counter_mem.groupInfo.key = "mem"
        mock_counter_mem.nameInfo.key = "usage"
        mock_counter_mem.key = 2

        mock_counter_disk = MagicMock()
        mock_counter_disk.groupInfo.key = "disk"
        mock_counter_disk.nameInfo.key = "usage"
        mock_counter_disk.key = 3
        
        mock_counter_net = MagicMock()
        mock_counter_net.groupInfo.key = "net"
        mock_counter_net.nameInfo.key = "usage"
        mock_counter_net.key = 4

        self.mock_perf_manager.perfCounter = [
            mock_counter_cpu, mock_counter_mem, mock_counter_disk, mock_counter_net
        ]
        
        # Mock RetrieveContent for ResourceMonitor methods that might call it
        # (e.g., _get_performance_data in its original form, though it's passed SI in __init__)
        self.mock_si.RetrieveContent = MagicMock()

        self.resource_monitor = ResourceMonitor(self.mock_si)

    @patch('modules.resource_monitor.logger.error') # Patch logger in the module where it's used
    def test_get_performance_data_handles_attribute_error(self, mock_logger_error):
        # Mock self.performance_manager.QueryPerf to raise AttributeError
        self.resource_monitor.performance_manager.QueryPerf = MagicMock(
            side_effect=AttributeError("Simulated Attribute Error 'str' object has no attribute '_moId'")
        )

        # Prepare a mock entity
        mock_entity = MagicMock(spec=vim.HostSystem) # Or any other managed entity
        mock_entity.name = "TestEntity"
        mock_entity._moId = "entity-moid-123"
        
        # Call _get_performance_data
        result = self.resource_monitor._get_performance_data(mock_entity, "cpu.usage")

        # Assertions
        self.assertEqual(result, 0, "Should return 0 on AttributeError")
        
        # Check logger call
        # Example: f"[_get_performance_data] AttributeError caught for entity '{entity_name_for_log}' (_moId: {getattr(entity, '_moId', 'N/A')}) during QueryPerf or result processing. Exact error: {str(ae)}"
        # We need to check parts of the message as the full message might be complex to match exactly
        self.assertTrue(mock_logger_error.called)
        
        # Check that some key parts of the expected log message are present in any of the calls to logger.error
        # The actual log message for AttributeError is:
        # f"[_get_performance_data] AttributeError caught for entity '{entity_name_for_log}' (_moId: {getattr(entity, '_moId', 'N/A')}) during QueryPerf or result processing. Exact error: {str(ae)}"
        # f"[_get_performance_data] Entity type processed was: {type(entity)}"
        
        args_list = mock_logger_error.call_args_list
        
        expected_msg_part1 = f"AttributeError caught for entity '{mock_entity.name}' (_moId: {mock_entity._moId}) during QueryPerf or result processing."
        expected_msg_part2 = "Simulated Attribute Error 'str' object has no attribute '_moId'" # The error itself
        expected_msg_part3 = f"Entity type processed was: {type(mock_entity)}"

        log_call1_found = any(
            expected_msg_part1 in call_args[0][0] and expected_msg_part2 in call_args[0][0]
            for call_args in args_list
        )
        log_call2_found = any(
            expected_msg_part3 in call_args[0][0]
            for call_args in args_list
        )

        self.assertTrue(log_call1_found, "AttributeError log message not found or incorrect.")
        self.assertTrue(log_call2_found, "Entity type log message for AttributeError not found or incorrect.")


    @patch('modules.resource_monitor.ResourceMonitor._get_performance_data', return_value=0)
    @patch('modules.resource_monitor.logger.warning') # To check for pNIC warnings
    def test_get_host_metrics_defaults_usage_on_perf_data_error(self, mock_logger_warning, mock_get_perf_data):
        # Prepare a mock host object
        mock_host = MagicMock(spec=vim.HostSystem)
        mock_host.name = "TestHost"
        
        # Hardware summary for capacity
        mock_host.summary.hardware.numCpuCores = 4
        mock_host.summary.hardware.cpuMhz = 2000  # MHz per core
        mock_host.summary.hardware.memorySize = 8 * 1024 * 1024 * 1024  # 8 GB in bytes
        
        # Network config (pNICs)
        # Scenario 1: Valid pNICs
        pnic1 = MagicMock()
        pnic1.linkSpeed.speedMb = 10000 # 10 Gbps
        pnic2 = MagicMock()
        pnic2.linkSpeed.speedMb = 10000 # 10 Gbps
        mock_host.config.network.pnic = [pnic1, pnic2]

        # Action
        metrics = self.resource_monitor.get_host_metrics(mock_host)

        # Assertions for usage metrics (should be 0.0 due to _get_performance_data returning 0)
        self.assertEqual(metrics['cpu_usage'], 0.0)
        self.assertEqual(metrics['memory_usage'], 0.0)
        self.assertEqual(metrics['disk_io_usage'], 0.0) # 0 / 1024.0
        self.assertEqual(metrics['network_io_usage'], 0.0) # 0 / 1024.0

        # Assertions for capacity metrics
        self.assertEqual(metrics['cpu_capacity'], 4 * 2000) # numCpuCores * cpuMhz
        self.assertEqual(metrics['memory_capacity'], (8 * 1024 * 1024 * 1024) / (1024 * 1024)) # B to MB
        self.assertEqual(metrics['disk_io_capacity'], 1000) # Default disk I/O capacity
        
        # Network capacity: (10000 + 10000) / 8.0 = 20000 / 8.0 = 2500.0 MB/s
        self.assertEqual(metrics['network_capacity'], 2500.0)
        mock_logger_warning.assert_not_called() # No warnings for pNICs in this case

        # Scenario 2: No pNIC information (should use default and log warning)
        mock_host.config.network.pnic = None # or [] or mock_host.config.network = None
        mock_get_perf_data.reset_mock() # Reset call count from previous
        
        metrics_no_pnic = self.resource_monitor.get_host_metrics(mock_host)
        self.assertEqual(metrics_no_pnic['network_capacity'], 1250.0) # Default 10 Gbps in MB/s
        mock_logger_warning.assert_any_call(
            f"Host '{mock_host.name}': Could not retrieve pNIC information. Defaulting network capacity."
        )
        mock_logger_warning.reset_mock()

        # Scenario 3: pNIC with invalid linkSpeed.speedMb (not an int)
        pnic_invalid_speed = MagicMock()
        pnic_invalid_speed.device = "vmnic_invalid"
        pnic_invalid_speed.linkSpeed.speedMb = "should_be_int" # Invalid type
        
        mock_host.config.network.pnic = [pnic_invalid_speed]
        metrics_invalid_pnic_speed = self.resource_monitor.get_host_metrics(mock_host)
        self.assertEqual(metrics_invalid_pnic_speed['network_capacity'], 1250.0) # Default
        mock_logger_warning.assert_any_call(
            f"Host '{mock_host.name}', pNIC '{pnic_invalid_speed.device}': linkSpeed.speedMb found but is not an integer (type: {type('should_be_int')} value: should_be_int). Skipping this pNIC for network capacity sum."
        )
        # Another warning because no valid speeds were found
        mock_logger_warning.assert_any_call(
            f"Host '{mock_host.name}': No valid integer link speeds (speedMb) found for pNICs. Defaulting network capacity."
        )

    @patch('modules.resource_monitor.logger.error')
    @patch('modules.resource_monitor.ResourceMonitor._get_performance_data', return_value=0)
    def test_get_host_metrics_capacity_fetching_error(self, mock_get_perf_data, mock_logger_error):
        # Prepare a mock host object designed to cause an error during capacity fetching
        mock_host_problematic = MagicMock(spec=vim.HostSystem)
        mock_host_problematic.name = "ProblemHost"
        
        # Cause an AttributeError when trying to access host.summary.hardware
        # One way: make summary an object that doesn't have 'hardware'
        # Another way: make summary itself raise an error, or summary.hardware be None
        mock_host_problematic.summary = MagicMock()
        # Make accessing summary.hardware raise an error
        mock_host_problematic.summary.hardware = None # This will cause AttributeError when accessing numCpuCores etc.
        # Or, for a more direct error on access of 'hardware':
        # type(mock_host_problematic.summary).hardware = PropertyMock(side_effect=AttributeError("Simulated hardware access error"))


        # Action
        metrics = self.resource_monitor.get_host_metrics(mock_host_problematic)

        # Assertions
        # 1. logger.error was called
        mock_logger_error.assert_called_once()
        # Check if the log message contains key phrases
        call_args_str = str(mock_logger_error.call_args)
        self.assertIn(f"Error fetching capacity for host '{mock_host_problematic.name}'", call_args_str)
        self.assertIn("Capacities will be defaulted.", call_args_str)
        
        # 2. Usage metrics are 0.0 (because _get_performance_data returns 0)
        self.assertEqual(metrics['cpu_usage'], 0.0)
        self.assertEqual(metrics['memory_usage'], 0.0)
        self.assertEqual(metrics['disk_io_usage'], 0.0)
        self.assertEqual(metrics['network_io_usage'], 0.0)

        # 3. Capacity metrics are defaulted
        self.assertEqual(metrics['cpu_capacity'], 0)
        self.assertEqual(metrics['memory_capacity'], 0)
        self.assertEqual(metrics['disk_io_capacity'], 1)
        self.assertEqual(metrics['network_capacity'], 1)


if __name__ == '__main__':
    unittest.main()
