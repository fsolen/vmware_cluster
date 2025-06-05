import unittest
from unittest.mock import MagicMock, patch
import logging

# Assuming modules are accessible, either via PYTHONPATH or relative imports if tests are run as a module
# For simplicity, let's assume direct import path works for the testing environment.
from modules.cluster_state import ClusterState
from pyVmomi import vim # Required for type checks if any, or creating specific vim mock objects

# Configure a logger for tests if needed, or patch the one in the module
# This logger can be used if we want to check specific log messages from 'fdrs'
logger = logging.getLogger('fdrs')
# To prevent logs from fdrs from appearing during tests unless explicitly desired for debugging
# logger.setLevel(logging.CRITICAL + 1)


class TestClusterState(unittest.TestCase):

    def setUp(self):
        # Mock service_instance for ClusterState initialization
        self.mock_si = MagicMock()
        
        # Prevent actual calls to _get_all_vms and _get_all_hosts during ClusterState init
        with patch.object(ClusterState, '_get_all_vms', return_value=[]), \
             patch.object(ClusterState, '_get_all_hosts', return_value=[]):
            self.cluster_state = ClusterState(self.mock_si)

    def test_get_vms_on_host_correctly_filters_vms(self):
        # --- Mocking Target Host ---
        mock_target_host = MagicMock(spec=vim.HostSystem) # Use spec for type hinting if needed
        mock_target_host.name = "target_host_name"
        mock_target_host._moId = "host-moid-target"

        # --- Mocking Other Host ---
        mock_other_host = MagicMock(spec=vim.HostSystem)
        mock_other_host.name = "other_host_name"
        mock_other_host._moId = "host-moid-other"
        
        # --- Mocking Host without _moId ---
        mock_host_no_moid = MagicMock(spec=vim.HostSystem)
        mock_host_no_moid.name = "host_no_moid_name"
        # Deliberately not setting _moId, or setting it to None if hasattr is used before gettattr
        # For the current implementation, hasattr(host_of_vm, '_moId') and host_of_vm._moId
        # means we need to ensure _moId is not just missing but evaluates to false if present
        del mock_host_no_moid._moId # To ensure hasattr returns False
        # or mock_host_no_moid._moId = None # If the check is just `if host_of_vm._moId:`

        # --- Mocking VMs ---
        vm_on_target_host = MagicMock(spec=vim.VirtualMachine)
        vm_on_target_host.name = "vm1_on_target"

        vm_on_other_host = MagicMock(spec=vim.VirtualMachine)
        vm_on_other_host.name = "vm2_on_other"

        vm_host_none = MagicMock(spec=vim.VirtualMachine)
        vm_host_none.name = "vm3_host_none"
        
        vm_host_no_moid_attr = MagicMock(spec=vim.VirtualMachine)
        vm_host_no_moid_attr.name = "vm4_host_no_moid"

        self.cluster_state.vms = [
            vm_on_target_host,
            vm_on_other_host,
            vm_host_none,
            vm_host_no_moid_attr
        ]

        # --- Mocking ClusterState.get_host_of_vm ---
        def mock_get_host_of_vm(vm_obj):
            if vm_obj == vm_on_target_host:
                return mock_target_host
            elif vm_obj == vm_on_other_host:
                return mock_other_host
            elif vm_obj == vm_host_none:
                return None
            elif vm_obj == vm_host_no_moid_attr:
                # This case is for when get_host_of_vm returns a host, but that host lacks a valid _moId
                return mock_host_no_moid 
            return None 

        self.cluster_state.get_host_of_vm = MagicMock(side_effect=mock_get_host_of_vm)

        # --- Test Scenario 1: VMs on target host ---
        result = self.cluster_state.get_vms_on_host(mock_target_host)
        self.assertIn(vm_on_target_host, result)
        self.assertEqual(len(result), 1, "Only vm_on_target_host should be returned")

        # --- Test Scenario 2: VM on a different host ---
        # (Covered by the previous assertion checking length)

        # --- Test Scenario 3: VM for which get_host_of_vm returns None ---
        # (Covered by the previous assertion checking length)
        
        # --- Test Scenario 4: VM whose host lacks _moId ---
        # (Covered by the previous assertion checking length, vm_host_no_moid_attr should not be in result)
        # We can add an explicit check if logger was called for this scenario
        with patch.object(logger, 'warning') as mock_log_warning:
            self.cluster_state.get_host_of_vm = MagicMock(side_effect=lambda vm_obj: mock_host_no_moid if vm_obj == vm_host_no_moid_attr else mock_target_host) # Simplify for this specific check
            self.cluster_state.vms = [vm_host_no_moid_attr] # Isolate the VM
            self.cluster_state.get_vms_on_host(mock_target_host) # Call the method
            
            # Check if warning was logged for vm_host_no_moid_attr's host lacking _moId
            # This part of the test assumes the logger is called when host_of_vm lacks _moId.
            # The current implementation of get_vms_on_host logs when host_of_vm is valid but _moId is not.
            # So, get_host_of_vm must return mock_host_no_moid for vm_host_no_moid_attr
            
            # Reset get_host_of_vm for other scenarios if needed, or re-initialize for clarity
            # For this specific sub-test, let's re-run with the specific logger check
            self.cluster_state.vms = [vm_on_target_host, vm_host_no_moid_attr]
            self.cluster_state.get_host_of_vm = MagicMock(side_effect=mock_get_host_of_vm) # use original comprehensive mock

            mock_log_warning.reset_mock() # Reset from previous calls if any
            
            # Make mock_host_no_moid have _moId = None to test the other branch of the `if host_of_vm and hasattr(host_of_vm, '_moId') and host_of_vm._moId:`
            mock_host_no_moid_with_none_moid = MagicMock(spec=vim.HostSystem)
            mock_host_no_moid_with_none_moid.name = "host_with_none_moid"
            mock_host_no_moid_with_none_moid._moId = None

            def Svc_mock_get_host_of_vm_for_none_moid(vm_obj):
                 if vm_obj == vm_host_no_moid_attr:
                     return mock_host_no_moid_with_none_moid
                 return None # Default for others
            
            self.cluster_state.get_host_of_vm = MagicMock(side_effect=Svc_mock_get_host_of_vm_for_none_moid)
            self.cluster_state.vms = [vm_host_no_moid_attr]
            
            self.cluster_state.get_vms_on_host(mock_target_host) # Call method
            
            mock_log_warning.assert_any_call(
                f"[ClusterState.get_vms_on_host] Host '{mock_host_no_moid_with_none_moid.name}' for VM '{vm_host_no_moid_attr.name}' is invalid or has no _moId. Skipping for host comparison."
            )


        # --- Test Scenario 5: Input host_object itself lacks _moId ---
        mock_target_host_no_moid = MagicMock(spec=vim.HostSystem)
        mock_target_host_no_moid.name = "target_host_no_moid"
        # del mock_target_host_no_moid._moId # To make hasattr(_moId) False
        # To test the `not host_object._moId` part when _moId exists but is None:
        mock_target_host_no_moid._moId = None


        with patch.object(logger, 'warning') as mock_log_warning:
            result = self.cluster_state.get_vms_on_host(mock_target_host_no_moid)
            self.assertEqual(result, [])
            mock_log_warning.assert_called_once_with(
                f"[ClusterState.get_vms_on_host] Provided host_object '{mock_target_host_no_moid.name}' is invalid or has no _moId. Cannot find VMs."
            )
        
        # Test with host_object where _moId attribute doesn't exist
        del mock_target_host_no_moid._moId 
        with patch.object(logger, 'warning') as mock_log_warning:
            result = self.cluster_state.get_vms_on_host(mock_target_host_no_moid)
            self.assertEqual(result, [])
            mock_log_warning.assert_called_once_with(
                f"[ClusterState.get_vms_on_host] Provided host_object '{mock_target_host_no_moid.name}' is invalid or has no _moId. Cannot find VMs."
            )

    # To patch the logger in the 'modules.cluster_state' module directly for get_host_by_name tests
    @patch('modules.cluster_state.logger')
    def test_get_host_by_name(self, mock_cs_logger):
        # Setup mock hosts
        mock_host1 = MagicMock(spec=vim.HostSystem)
        mock_host1.name = "host-01.example.com"
        mock_host2 = MagicMock(spec=vim.HostSystem)
        mock_host2.name = "host-02.example.com"

        self.cluster_state.hosts = [mock_host1, mock_host2]

        # Scenario 1: Host Found
        found_host = self.cluster_state.get_host_by_name("host-01.example.com")
        self.assertIs(found_host, mock_host1)
        mock_cs_logger.warning.assert_not_called()

        # Scenario 2: Host Not Found
        mock_cs_logger.reset_mock() # Reset mock for the next assertion
        unknown_host = self.cluster_state.get_host_by_name("unknown-host.example.com")
        self.assertIsNone(unknown_host)
        mock_cs_logger.warning.assert_called_once_with(
            "[ClusterState.get_host_by_name] Host 'unknown-host.example.com' not found in self.hosts."
        )

        # Scenario 3: self.hosts is Empty
        mock_cs_logger.reset_mock()
        self.cluster_state.hosts = []
        empty_hosts_result = self.cluster_state.get_host_by_name("host-01.example.com")
        self.assertIsNone(empty_hosts_result)
        mock_cs_logger.warning.assert_called_once_with(
            "[ClusterState.get_host_by_name] self.hosts is not initialized or is empty."
        )

        # Scenario 4: self.hosts is None
        mock_cs_logger.reset_mock()
        self.cluster_state.hosts = None
        none_hosts_result = self.cluster_state.get_host_by_name("host-01.example.com")
        self.assertIsNone(none_hosts_result)
        mock_cs_logger.warning.assert_called_once_with(
            "[ClusterState.get_host_by_name] self.hosts is not initialized or is empty."
        )
        
        # Scenario 5: Host object in list lacks 'name' attribute
        mock_cs_logger.reset_mock()
        mock_host_no_name = MagicMock(spec_set=['_moId']) # spec_set ensures only specified attrs exist
                                                       # Does not have 'name'
        
        # Re-populate hosts for this scenario
        self.cluster_state.hosts = [mock_host_no_name, mock_host1]

        # Try to find the host with a name
        found_host_after_no_name = self.cluster_state.get_host_by_name("host-01.example.com")
        self.assertIs(found_host_after_no_name, mock_host1)
        mock_cs_logger.warning.assert_not_called() # Should not log for successfully finding mock_host1

        # Try to find a name that would "match" the no-name host if it had one
        mock_cs_logger.reset_mock()
        no_name_found_result = self.cluster_state.get_host_by_name("some_name_for_no_name_host")
        self.assertIsNone(no_name_found_result)
        mock_cs_logger.warning.assert_called_once_with(
            "[ClusterState.get_host_by_name] Host 'some_name_for_no_name_host' not found in self.hosts."
        )

    @patch('modules.cluster_state.logger')
    def test_log_cluster_stats_with_defaulted_capacities(self, mock_cs_logger):
        # Setup host_metrics with defaulted capacities
        self.cluster_state.host_metrics = {
            "host1.example.com": {
                'cpu_usage': 1000, 'memory_usage': 2048, 'disk_io_usage': 50, 'network_io_usage': 20, # Some usage
                'cpu_capacity': 0, 'memory_capacity': 0, 'disk_io_capacity': 1, 'network_capacity': 1, # Defaulted capacities
                'cpu_usage_pct': 0, 'memory_usage_pct': 0, # Percentages will be 0 due to 0 capacity
                'vms': ["vm1", "vm2"]
            }
        }
        # Setup vm_metrics (can be empty or with some data, does not affect this specific test focus)
        self.cluster_state.vm_metrics = {
            "vm1": {"vm_obj": MagicMock(name="vm1"), "cpu_usage_abs": 500, "memory_usage_abs": 1024, "disk_io_usage_abs": 20, "network_io_usage_abs": 10},
            "vm2": {"vm_obj": MagicMock(name="vm2"), "cpu_usage_abs": 500, "memory_usage_abs": 1024, "disk_io_usage_abs": 30, "network_io_usage_abs": 10}
        }
        
        # Mock get_host_of_vm for VM logging part (if vm_metrics is not empty)
        mock_vm1_host = MagicMock(spec=vim.HostSystem)
        mock_vm1_host.name = "host1.example.com"
        self.cluster_state.get_host_of_vm = MagicMock(return_value=mock_vm1_host)


        # Action
        self.cluster_state.log_cluster_stats()

        # Assertions
        # Check that host-level stats are logged
        mock_cs_logger.info.assert_any_call("\n--- Host Resource Distribution ---")
        mock_cs_logger.info.assert_any_call("\nHost: host1.example.com")
        
        # Check CPU line: Since capacity is 0, usage % should be 0
        # Format: f"├─ CPU: {metrics['cpu_usage_pct']:.1f}% ({metrics['cpu_usage']}/{metrics['cpu_capacity']} MHz)"
        mock_cs_logger.info.assert_any_call("├─ CPU: 0.0% (1000/0 MHz)")
        
        # Check Memory line: Since capacity is 0, usage % should be 0
        # Format: f"├─ Memory: {metrics['memory_usage_pct']:.1f}% ({metrics['memory_usage']}/{metrics['memory_capacity']} MB)"
        mock_cs_logger.info.assert_any_call("├─ Memory: 0.0% (2048/0 MB)")

        # Check Disk I/O line: Capacity is 1
        # Format: f"├─ Disk I/O: {metrics['disk_io_usage']:.1f} MBps" 
        # Note: The log does not show disk capacity directly in this line, but it's used for pct calc elsewhere if implemented.
        # Here we just check the usage is logged.
        mock_cs_logger.info.assert_any_call("├─ Disk I/O: 50.0 MBps")

        # Check Network I/O line: Capacity is 1
        # Format: f"├─ Network I/O: {metrics['network_io_usage']:.1f} MBps"
        mock_cs_logger.info.assert_any_call("├─ Network I/O: 20.0 MBps")
        
        mock_cs_logger.info.assert_any_call("└─ VMs: 2 (vm1, vm2)")

        # Check overall cluster totals (capacities will be 0 or 1, affecting percentages)
        mock_cs_logger.info.assert_any_call("\n=== Cluster Total Resource Usage ===")
        mock_cs_logger.info.assert_any_call("CPU: 0.0% (1000/0 MHz)") # Total capacity is 0
        mock_cs_logger.info.assert_any_call("Memory: 0.0% (2048/0 MB)") # Total capacity is 0
        mock_cs_logger.info.assert_any_call("Total Disk I/O: 50.0 MBps")
        mock_cs_logger.info.assert_any_call("Total Network I/O: 20.0 MBps")

    def test_annotate_hosts_memory_usage_from_host_summary(self):
        """
        Verify that host memory_usage is taken from host.summary.quickStats.overallMemoryUsage
        and not summed from VM guest memory.
        """
        # 1. Mock ResourceMonitor
        mock_resource_monitor = MagicMock()
        # Define what get_host_metrics returns for capacities
        # Note: memory_capacity from get_host_metrics is host.summary.hardware.memorySize / (1024*1024)
        # overallMemoryUsage is in MB.
        mock_resource_monitor.get_host_metrics.return_value = {
            'cpu_capacity': 2000, # MHz
            'memory_capacity': 4096, # MB (e.g. 4GB host, this value is expected after division)
            'disk_io_capacity': 100, # MBps
            'network_capacity': 1000 # Mbps
        }

        # 2. Create Mock Host
        mock_host1 = MagicMock(spec=vim.HostSystem)
        mock_host1.name = "host1"
        mock_host1._moId = "host-moid-1"
        mock_host1.summary = MagicMock()
        mock_host1.summary.quickStats = MagicMock()
        mock_host1.summary.quickStats.overallMemoryUsage = 2048 # Host reports 2048 MB used (Consumed)
        # The memorySize would be used by ResourceMonitor to calculate 'memory_capacity'
        # For this test, we mock get_host_metrics to provide memory_capacity directly.

        # 3. Create Mock VMs for this host
        mock_vm1 = MagicMock(spec=vim.VirtualMachine)
        mock_vm1.name = "vm1_on_host1"
        mock_vm1._moId = "vm-moid-1"
        mock_vm1.runtime = MagicMock()
        mock_vm1.runtime.host = mock_host1
        mock_vm1.summary = MagicMock()
        mock_vm1.summary.quickStats = MagicMock()
        mock_vm1.summary.quickStats.guestMemoryUsage = 512 # VM Guest active memory (should NOT be used for host total)
        mock_vm1.summary.quickStats.overallCpuUsage = 100 # VM CPU usage (should be summed)

        mock_vm2 = MagicMock(spec=vim.VirtualMachine)
        mock_vm2.name = "vm2_on_host1"
        mock_vm2._moId = "vm-moid-2"
        mock_vm2.runtime = MagicMock()
        mock_vm2.runtime.host = mock_host1
        mock_vm2.summary = MagicMock()
        mock_vm2.summary.quickStats = MagicMock()
        mock_vm2.summary.quickStats.guestMemoryUsage = 1024 # VM Guest active memory
        mock_vm2.summary.quickStats.overallCpuUsage = 200 # VM CPU usage

        # 4. Initialize ClusterState and its attributes
        # Use a fresh instance for this test, or ensure setUp is appropriate
        # For simplicity, create a new one, or carefully manage self.cluster_state from setUp
        # We will use self.cluster_state from setUp, but override its hosts and vms for this test
        self.cluster_state.hosts = [mock_host1]
        self.cluster_state.vms = [mock_vm1, mock_vm2]

        # Pre-populate vm_metrics (as done by annotate_vms_with_metrics)
        # disk_io_usage_abs and network_io_usage_abs are mocked from resource_monitor.get_vm_metrics
        # For this test, let's assume they are 0 or some value, focus is on memory.
        with patch.object(mock_resource_monitor, 'get_vm_metrics') as mock_get_vm_metrics:
            mock_get_vm_metrics.side_effect = lambda vm: {
                'disk_io_usage': 10 if vm == mock_vm1 else 5, # Example values
                'network_io_usage': 2 if vm == mock_vm1 else 1
            }
            # Call annotate_vms_with_metrics to populate self.cluster_state.vm_metrics
            self.cluster_state.annotate_vms_with_metrics(mock_resource_monitor)


        # Ensure get_vms_on_host returns the correct VMs for the host
        self.cluster_state.get_vms_on_host = MagicMock(return_value=[mock_vm1, mock_vm2])

        # 5. Call annotate_hosts_with_metrics
        self.cluster_state.annotate_hosts_with_metrics(mock_resource_monitor)

        # 6. Assertions
        host_metrics_data = self.cluster_state.host_metrics.get("host1")
        self.assertIsNotNone(host_metrics_data)

        # Assert Memory Usage (Primary test goal)
        # Should be host's overallMemoryUsage, not sum of VMs' guestMemoryUsage (512+1024=1536)
        self.assertEqual(host_metrics_data['memory_usage'], 2048)

        # Assert Memory Capacity (comes from mocked get_host_metrics)
        self.assertEqual(host_metrics_data['memory_capacity'], 4096)

        # Assert Memory Usage Percentage
        expected_mem_pct = (2048 / 4096) * 100 if 4096 > 0 else 0
        self.assertAlmostEqual(host_metrics_data['memory_usage_pct'], expected_mem_pct)

        # Assert CPU Usage (should still be sum of VM CPU)
        # vm1 CPU = 100, vm2 CPU = 200. Total = 300.
        self.assertEqual(host_metrics_data['cpu_usage'], 300)
        expected_cpu_pct = (300 / 2000) * 100 if 2000 > 0 else 0
        self.assertAlmostEqual(host_metrics_data['cpu_usage_pct'], expected_cpu_pct)

        # Assert Disk and Network are summed (based on mock_get_vm_metrics)
        # Disk: 10 (vm1) + 5 (vm2) = 15
        # Network: 2 (vm1) + 1 (vm2) = 3
        self.assertEqual(host_metrics_data['disk_io_usage'], 15)
        self.assertEqual(host_metrics_data['network_io_usage'], 3)


if __name__ == '__main__':
    unittest.main()
