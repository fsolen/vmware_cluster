import unittest
from unittest.mock import MagicMock, patch

from modules.constraint_manager import ConstraintManager
from modules.cluster_state import ClusterState # Needed for type hinting if used, and its methods are mocked
from pyVmomi import vim # For creating vim object mocks if necessary for type consistency

# Get the logger instance from the module to be tested if you want to assert its calls specifically
# from modules import constraint_manager as cm_module_logger_ref (example, if logger was module level there)
# For now, assume logger is 'fdrs' and can be patched via 'modules.constraint_manager.logger'
# import logging
# logger = logging.getLogger('fdrs') # This is the logger used by the module


class TestConstraintManager(unittest.TestCase):

    def setUp(self):
        self.mock_cluster_state = MagicMock(spec=ClusterState)
        self.constraint_manager = ConstraintManager(self.mock_cluster_state)

        # Mock hosts
        self.mock_host1 = MagicMock(spec=vim.HostSystem)
        self.mock_host1.name = "H1"
        self.mock_host1._moId = "host-moid-1"

        self.mock_host2 = MagicMock(spec=vim.HostSystem)
        self.mock_host2.name = "H2"
        self.mock_host2._moId = "host-moid-2"

        self.mock_host3 = MagicMock(spec=vim.HostSystem)
        self.mock_host3.name = "H3"
        self.mock_host3._moId = "host-moid-3"
        
        self.mock_host4 = MagicMock(spec=vim.HostSystem)
        self.mock_host4.name = "H4"
        self.mock_host4._moId = "host-moid-4"

        self.all_hosts = [self.mock_host1, self.mock_host2, self.mock_host3, self.mock_host4]
        self.mock_cluster_state.hosts = self.all_hosts

        # Mock VMs
        self.vms = []
        for i in range(1, 7): # V1 to V6
            vm = MagicMock(spec=vim.VirtualMachine)
            vm.name = f"testvm0{i}" # All in 'testvm0' group
            self.vms.append(vm)
        
        self.constraint_manager.vm_distribution = {
            "testvm0": self.vms # All 6 VMs in one group
        }
        self.mock_cluster_state.vms = self.vms # Make VMs available to cluster_state

    # Helper to set VM locations for a test
    def _set_vm_hosts(self, vm_host_map):
        # vm_host_map is like {vm_object: host_object}
        # Ensure get_host_of_vm returns the correct host for each VM
        # and also correctly populates current_host_group_counts within get_preferred_host_for_vm
        
        # Store the map for get_host_of_vm
        self.current_vm_host_map = vm_host_map
        
        def mock_get_host_of_vm(vm_obj):
            return self.current_vm_host_map.get(vm_obj)
            
        self.mock_cluster_state.get_host_of_vm = MagicMock(side_effect=mock_get_host_of_vm)


    @patch('modules.constraint_manager.logger') # Patch the logger in constraint_manager module
    def test_get_preferred_host_perfect_move_found(self, mock_logger):
        # Scenario 1: H1={V1,V2,V3}, H2={V4}, H3={V5}, H4={V6} (3,1,1,1 - skew 2). VM to move: V1 from H1.
        # Moving V1 to H2 -> (V2,V3), (V4,V1), (V5), (V6) -> (2,2,1,1) - skew 1 (Perfect)
        # Moving V1 to H3 -> (V2,V3), (V4), (V5,V1), (V6) -> (2,1,2,1) - skew 1 (Perfect)
        # Moving V1 to H4 -> (V2,V3), (V4), (V5), (V6,V1) -> (2,1,1,2) - skew 1 (Perfect)
        # All H2, H3, H4 are perfect. H2 has 1 VM from group, H3 has 1, H4 has 1.
        # Tie-breaking by name: H2 < H3 < H4. So H2 should be chosen.
        vm_map = {
            self.vms[0]: self.mock_host1, self.vms[1]: self.mock_host1, self.vms[2]: self.mock_host1, # V1,V2,V3 on H1
            self.vms[3]: self.mock_host2, # V4 on H2
            self.vms[4]: self.mock_host3, # V5 on H3
            self.vms[5]: self.mock_host4  # V6 on H4
        }
        self._set_vm_hosts(vm_map)
        
        vm_to_move = self.vms[0] # V1 from H1
        preferred_host = self.constraint_manager.get_preferred_host_for_vm(vm_to_move)
        
        self.assertIsNotNone(preferred_host, "Should find a preferred host.")
        self.assertEqual(preferred_host.name, "H2", "H2 should be chosen for perfect balance due to tie-breaking.")
        mock_logger.info.assert_any_call(f"[ConstraintManager] Preferred host for VM '{vm_to_move.name}' (perfect balance) is '{preferred_host.name}'.")


    @patch('modules.constraint_manager.logger')
    def test_get_preferred_host_greedy_move_found(self, mock_logger):
        # Scenario 2: H1={V1,V2,V3,V4}, H2={V5,V6}, H3={}, H4={} (4,2,0,0 - skew 4). VM to move: V1 from H1.
        # No perfect move:
        # V1 to H2 -> (3,3,0,0) skew 3
        # V1 to H3 -> (3,2,1,0) skew 3
        # V1 to H4 -> (3,2,0,1) skew 3
        # Greedy: H3 and H4 have 0 group VMs, H1 has 4. 0 < 4.
        # Tie-breaking by name: H3 < H4. So H3 should be chosen.
        vm_map = {
            self.vms[0]: self.mock_host1, self.vms[1]: self.mock_host1, self.vms[2]: self.mock_host1, self.vms[3]: self.mock_host1, # V1-V4 on H1
            self.vms[4]: self.mock_host2, self.vms[5]: self.mock_host2, # V5,V6 on H2
            # H3, H4 have no VMs from this group
        }
        self._set_vm_hosts(vm_map)

        vm_to_move = self.vms[0] # V1 from H1
        preferred_host = self.constraint_manager.get_preferred_host_for_vm(vm_to_move)

        self.assertIsNotNone(preferred_host, "Should find a preferred host.")
        self.assertEqual(preferred_host.name, "H3", "H3 should be chosen for greedy move.")
        mock_logger.info.assert_any_call(f"[ConstraintManager] No host achieves perfect AA balance. Selecting host '{preferred_host.name}' to reduce load from source for VM '{vm_to_move.name}'.")

    @patch('modules.constraint_manager.logger')
    def test_get_preferred_host_no_improvement_but_less_than_source(self, mock_logger):
        # Scenario 3: H1={V1,V2,V3}, H2={V4,V5,V6}, H3={}, H4={} (3,3,0,0 - skew 3). VM to move: V1 from H1.
        # No perfect move:
        # V1 to H3 -> (2,3,1,0) skew 3
        # V1 to H4 -> (2,3,0,1) skew 3
        # Greedy: H3 and H4 have 0 group VMs, H1 has 3. 0 < 3.
        # Tie-breaking by name: H3 < H4. So H3 should be chosen.
        vm_map = {
            self.vms[0]: self.mock_host1, self.vms[1]: self.mock_host1, self.vms[2]: self.mock_host1, # V1,V2,V3 on H1
            self.vms[3]: self.mock_host2, self.vms[4]: self.mock_host2, self.vms[5]: self.mock_host2, # V4,V5,V6 on H2
        }
        self._set_vm_hosts(vm_map)

        vm_to_move = self.vms[0] # V1 from H1
        preferred_host = self.constraint_manager.get_preferred_host_for_vm(vm_to_move)

        self.assertIsNotNone(preferred_host, "Should find a preferred host.")
        self.assertEqual(preferred_host.name, "H3", "H3 should be chosen for greedy move.")
        mock_logger.info.assert_any_call(f"[ConstraintManager] No host achieves perfect AA balance. Selecting host '{preferred_host.name}' to reduce load from source for VM '{vm_to_move.name}'.")


    @patch('modules.constraint_manager.logger')
    def test_get_preferred_host_source_only_host_with_vms(self, mock_logger):
        # Scenario 4: H1={V1..V6}, H2={}, H3={}, H4={}. VM to move V1 from H1.
        # Perfect moves:
        # V1 to H2 -> (5,1,0,0) skew 5
        # V1 to H3 -> (5,0,1,0) skew 5
        # V1 to H4 -> (5,0,0,1) skew 5
        # The logic first tries for perfect balance (sim_max_count - sim_min_count <=1).
        # If V1 moves to H2: counts are H1=5, H2=1, H3=0, H4=0. Max=5, Min=0. Diff=5. Not perfect.
        # This scenario will fall into the second pass (greedy).
        # Greedy: H2,H3,H4 all have 0 VMs. H1 has 6. 0 < 6.
        # Tie-breaking by name: H2 < H3 < H4. So H2 should be chosen.
        vm_map = { vm: self.mock_host1 for vm in self.vms } # All 6 VMs on H1
        self._set_vm_hosts(vm_map)

        vm_to_move = self.vms[0] # V1 from H1
        preferred_host = self.constraint_manager.get_preferred_host_for_vm(vm_to_move)

        self.assertIsNotNone(preferred_host, "Should find a preferred host.")
        self.assertEqual(preferred_host.name, "H2", "H2 should be chosen for greedy move.")
        mock_logger.info.assert_any_call(f"[ConstraintManager] No host achieves perfect AA balance. Selecting host '{preferred_host.name}' to reduce load from source for VM '{vm_to_move.name}'.")


if __name__ == '__main__':
    unittest.main()
