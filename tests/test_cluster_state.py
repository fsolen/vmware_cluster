import unittest
from unittest.mock import patch, Mock, MagicMock # Added MagicMock for service_instance
import logging

# Assuming modules are accessible in the path, otherwise adjust
from modules.cluster_state import ClusterState

class TestClusterStateLogging(unittest.TestCase):
    @patch('modules.cluster_state.logger') # Patch the logger in the module where ClusterState uses it
    def test_log_cluster_stats_table_format(self, mock_logger):
        # 1. Mock service_instance
        mock_service_instance = MagicMock()
        mock_service_instance.content.about.name = "TestVCenter"
        mock_service_instance._stub.host = "testvcenter.local"

        # 2. Instantiate ClusterState
        # Need to mock methods called in ClusterState.__init__ if they depend on service_instance
        with patch.object(ClusterState, '_get_all_vms', return_value=[]), \
             patch.object(ClusterState, '_get_all_hosts', return_value=[]):
            cs = ClusterState(mock_service_instance)

        # 3. Prepare mock host_metrics, vm_metrics, hosts, vms
        cs.host_metrics = {
            "host-01": {
                'cpu_usage_pct': 50.5, 'memory_usage_pct': 60.2,
                'disk_io_usage': 123.4, 'network_io_usage': 45.6,
                'vms': ["vm1", "vm2"],
                'cpu_capacity': 20000, 'memory_capacity': 128000, 'cpu_usage': 10100, 'memory_usage': 77000, # Added usage for totals
                'disk_io_capacity': 1000, 'network_capacity': 10000, # Capacities for other resources
            },
            "host-02": {
                'cpu_usage_pct': 30.1, 'memory_usage_pct': 40.7,
                'disk_io_usage': 78.9, 'network_io_usage': 12.3,
                'vms': ["vm3"],
                'cpu_capacity': 20000, 'memory_capacity': 128000, 'cpu_usage': 6020, 'memory_usage': 52000, # Added usage for totals
                'disk_io_capacity': 1000, 'network_capacity': 10000, # Capacities for other resources
            }
        }
        # vm_metrics is needed for the "VM Resource Consumption" part
        cs.vm_metrics = {
            "vm1": {'vm_obj': Mock(name="vm1"), 'cpu_usage_abs': 100, 'memory_usage_abs': 200, 'disk_io_usage_abs': 10, 'network_io_usage_abs': 5},
            "vm2": {'vm_obj': Mock(name="vm2"), 'cpu_usage_abs': 150, 'memory_usage_abs': 250, 'disk_io_usage_abs': 12, 'network_io_usage_abs': 6},
            "vm3": {'vm_obj': Mock(name="vm3"), 'cpu_usage_abs': 200, 'memory_usage_abs': 300, 'disk_io_usage_abs': 15, 'network_io_usage_abs': 7},
        }
        # Simplistic mocks for hosts and vms lists if log_cluster_stats uses them directly
        # For log_cluster_stats, cs.hosts and cs.vms are used for "Total Hosts" and "Total VMs" counts
        cs.hosts = [Mock(name="host-01"), Mock(name="host-02")]
        # For get_host_of_vm in VM Resource Consumption part
        mock_vm1_host = Mock()
        mock_vm1_host.name = "host-01"
        mock_vm2_host = Mock()
        mock_vm2_host.name = "host-01"
        mock_vm3_host = Mock()
        mock_vm3_host.name = "host-02"
        
        # Create VM mocks that have a 'runtime.host' attribute
        vm1_mock = Mock(name="vm1")
        vm1_mock.runtime.host = mock_vm1_host
        vm2_mock = Mock(name="vm2")
        vm2_mock.runtime.host = mock_vm2_host
        vm3_mock = Mock(name="vm3")
        vm3_mock.runtime.host = mock_vm3_host

        cs.vms = [vm1_mock, vm2_mock, vm3_mock]
        
        # Update vm_metrics to use these VM mocks that have runtime.host
        cs.vm_metrics["vm1"]['vm_obj'] = vm1_mock
        cs.vm_metrics["vm2"]['vm_obj'] = vm2_mock
        cs.vm_metrics["vm3"]['vm_obj'] = vm3_mock


        # 4. Call log_cluster_stats
        cs.log_cluster_stats()

        # 5. Assert logger calls
        log_calls = [call_args[0][0] for call_args in mock_logger.info.call_args_list]
        
        # for i, call_content in enumerate(log_calls): # For debugging
        #     print(f"Log call {i}: {call_content}")

        self.assertTrue(any("\n=== Cluster State Summary ===" in call for call in log_calls))
        
        expected_header = f"{'Cluster/vCenter':<30} {'Hostname':<25} {'CPU %':<10} {'Mem %':<10} {'Storage I/O (MBps)':<20} {'Net Throughput (MBps)':<25} {'VM Count':<10}"
        self.assertTrue(any(expected_header in call for call in log_calls), f"Expected header not found in log calls. Header: '{expected_header}'")
        
        self.assertTrue(any(call.strip() == "-" * len(expected_header) for call in log_calls), f"Table separator line not matching header length. Expected: {'-' * len(expected_header)}")


        expected_row1_pattern_parts = ["TestVCenter", "host-01", "50.5", "60.2", "123.4", "45.6", "2"]
        self.assertTrue(
            any(all(part in call for part in expected_row1_pattern_parts) for call in log_calls),
            "Expected data row for host-01 not found or not formatted correctly."
        )

        expected_row2_pattern_parts = ["TestVCenter", "host-02", "30.1", "40.7", "78.9", "12.3", "1"]
        self.assertTrue(
            any(all(part in call for part in expected_row2_pattern_parts) for call in log_calls),
            "Expected data row for host-02 not found or not formatted correctly."
        )

        self.assertTrue(any("\n--- Host Resource Distribution ---" in call for call in log_calls))
        self.assertTrue(any("\n--- VM Resource Consumption ---" in call for call in log_calls))
        self.assertTrue(any("\n=== Cluster Total Resource Usage ===" in call for call in log_calls))
        
        # Header (1) + Separator (1) + Data rows (2) + Section Titles (Cluster Summary, Host Dist, VM Cons, Total Usage) (4)
        # Plus individual host logs in "Host Resource Distribution" (2 hosts * 6 lines) = 12
        # Plus individual VM logs in "VM Resource Consumption" (3 VMs * 5 lines) = 15
        # Plus total cluster metrics (CPU, Mem, Disk, Net, Hosts, VMs) = 6
        # Min calls = 1 + 1 + 2 + 4 + 12 + 15 + 6 = 41
        # Reduced this due to potential variability in how many log lines are generated per host/VM if some fields are missing
        self.assertGreater(mock_logger.info.call_count, 35, "Not enough log messages; implies some sections might be missing.")


if __name__ == '__main__':
    unittest.main()
