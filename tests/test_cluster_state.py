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


if __name__ == '__main__':
    unittest.main()
