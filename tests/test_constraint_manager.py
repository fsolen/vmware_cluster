import unittest
from unittest.mock import MagicMock, patch
from modules.constraint_manager import ConstraintManager

# Basic Host and VM object structures for mocking
class MockHost:
    def __init__(self, name, vms=None):
        self.name = name
        self.vms = vms if vms else []

class MockVM:
    def __init__(self, name, host=None, config=None):
        self.name = name
        self.host = host
        self.config = config or MagicMock() # Allow setting template attribute etc.

class TestConstraintManager(unittest.TestCase):
    def setUp(self):
        self.mock_cluster_state = MagicMock()
        self.constraint_manager = ConstraintManager(self.mock_cluster_state)

        # Sample hosts
        self.host1 = MockHost(name="host1")
        self.host2 = MockHost(name="host2")
        self.host3 = MockHost(name="host3")
        self.active_hosts = [self.host1, self.host2, self.host3]
        self.mock_cluster_state.hosts = self.active_hosts
        
        # Sample VMs
        self.vm1_g1 = MockVM(name="vmA01")
        self.vm2_g1 = MockVM(name="vmA02")
        self.vm3_g1 = MockVM(name="vmA03")
        self.vm4_g2 = MockVM(name="vmB01")


    def test_find_perfect_balance_host_achievable(self):
        """Test _find_perfect_balance_host when perfect balance is achievable."""
        vm_to_move = self.vm1_g1 # From group 'vmA'
        current_host_group_counts = {'host1': 2, 'host2': 0, 'host3': 0} # vm1_g1, vm2_g1 on host1
        source_host_name = "host1"
        
        # Expected: moving vm1_g1 to host2 (1,1,0) or host3 (1,0,1) achieves perfect balance.
        # Host2 is lexicographically smaller than Host3 if counts are equal.
        # If vm1_g1 moves to host2: host1=1, host2=1, host3=0. Max-Min = 1.
        
        # Mock cluster state methods if necessary (not directly used by this helper, but good practice)
        self.mock_cluster_state.get_host_of_vm.return_value = self.host1

        target_host = self.constraint_manager._find_perfect_balance_host(
            vm_to_move, current_host_group_counts, source_host_name, self.active_hosts
        )
        self.assertIsNotNone(target_host)
        self.assertEqual(target_host.name, "host2") # Prefers host2 due to lower current count (0)

    def test_find_perfect_balance_host_not_achievable(self):
        """Test _find_perfect_balance_host when no host achieves perfect balance."""
        vm_to_move = self.vm1_g1
        # Scenario: Host1 has 3 VMs (vm1, vm2, vm3), Host2 has 0, Host3 has 0. Vm group size = 3
        # Moving one VM from Host1 (now 2) to Host2 (now 1) -> counts (2,1,0). Max-Min = 2. Not perfect.
        current_host_group_counts = {'host1': 3, 'host2': 0, 'host3': 0}
        source_host_name = "host1"
        
        target_host = self.constraint_manager._find_perfect_balance_host(
            vm_to_move, current_host_group_counts, source_host_name, self.active_hosts
        )
        self.assertIsNone(target_host)

    def test_find_perfect_balance_multiple_candidates_tie_breaking(self):
        """Test _find_perfect_balance_host with multiple candidates and tie-breaking."""
        vm_to_move = self.vm1_g1
        # Host1 has 2 (vm1, vm2), Host2 has 0, Host3 has 0. Vm group size = 2
        # Moving vm1 from host1 (now 1) to host2 (now 1) -> (1,1,0) - perfect.
        # Moving vm1 from host1 (now 1) to host3 (now 1) -> (1,0,1) - perfect.
        # Both host2 and host3 currently have 0 VMs of the group.
        # Tie-breaking: host with lexicographically smaller name (host2).
        current_host_group_counts = {'host1': 2, 'host2': 0, 'host3': 0}
        source_host_name = "host1"

        target_host = self.constraint_manager._find_perfect_balance_host(
            vm_to_move, current_host_group_counts, source_host_name, self.active_hosts
        )
        self.assertIsNotNone(target_host)
        self.assertEqual(target_host.name, "host2")


    def test_find_better_than_source_host_exists(self):
        """Test _find_better_than_source_host when a better host exists."""
        vm_to_move = self.vm1_g1
        current_host_group_counts = {'host1': 3, 'host2': 1, 'host3': 0} # Source host1 has 3.
        source_host_name = "host1"
        source_host_group_count = 3

        # host2 (1) and host3 (0) are better than source_host_group_count (3).
        # host3 is the best as it has the minimum (0).
        target_host = self.constraint_manager._find_better_than_source_host(
            vm_to_move, current_host_group_counts, source_host_name, source_host_group_count, self.active_hosts
        )
        self.assertIsNotNone(target_host)
        self.assertEqual(target_host.name, "host3")

    def test_find_better_than_source_host_none_better(self):
        """Test _find_better_than_source_host when no host is better than the source."""
        vm_to_move = self.vm1_g1
        current_host_group_counts = {'host1': 1, 'host2': 2, 'host3': 2} # Source host1 has 1.
        source_host_name = "host1"
        source_host_group_count = 1
        
        target_host = self.constraint_manager._find_better_than_source_host(
            vm_to_move, current_host_group_counts, source_host_name, source_host_group_count, self.active_hosts
        )
        self.assertIsNone(target_host)

    def test_find_better_than_source_multiple_better_tie_breaking(self):
        """Test _find_better_than_source_host with multiple better hosts and tie-breaking."""
        vm_to_move = self.vm1_g1
        # Source host1 has 3. Host2 has 1, Host3 has 1. Both are better.
        # Tie-breaking by name: host2.
        current_host_group_counts = {'host1': 3, 'host2': 1, 'host3': 1}
        source_host_name = "host1"
        source_host_group_count = 3

        target_host = self.constraint_manager._find_better_than_source_host(
            vm_to_move, current_host_group_counts, source_host_name, source_host_group_count, self.active_hosts
        )
        self.assertIsNotNone(target_host)
        self.assertEqual(target_host.name, "host2")

    @patch('modules.constraint_manager.ConstraintManager._find_better_than_source_host')
    @patch('modules.constraint_manager.ConstraintManager._find_perfect_balance_host')
    def test_get_preferred_host_for_vm_orchestration(self, mock_find_perfect, mock_find_better):
        """Test get_preferred_host_for_vm orchestration of helper calls."""
        vm_to_move = self.vm1_g1
        vm_to_move.name = "vmA01" # Ensure name is set for prefix logic

        # Setup for vm_distribution population
        self.mock_cluster_state.vms = [self.vm1_g1, self.vm2_g1]
        self.mock_cluster_state.get_host_of_vm.side_effect = lambda vm: {
            self.vm1_g1: self.host1, self.vm2_g1: self.host1
        }.get(vm)

        # Scenario 1: Perfect balance host found
        mock_find_perfect.return_value = self.host2
        result_host = self.constraint_manager.get_preferred_host_for_vm(vm_to_move)
        mock_find_perfect.assert_called_once()
        mock_find_better.assert_not_called()
        self.assertEqual(result_host, self.host2)

        # Reset mocks for Scenario 2
        mock_find_perfect.reset_mock()
        mock_find_better.reset_mock()
        mock_find_perfect.return_value = None # No perfect balance host
        mock_find_better.return_value = self.host3 # Better host found by second helper

        result_host = self.constraint_manager.get_preferred_host_for_vm(vm_to_move)
        mock_find_perfect.assert_called_once()
        mock_find_better.assert_called_once()
        self.assertEqual(result_host, self.host3)

    def test_get_preferred_host_for_vm_initial_checks_vm_invalid(self):
        invalid_vm = MockVM(name="v") # Name too short
        self.assertIsNone(self.constraint_manager.get_preferred_host_for_vm(invalid_vm))

        # VM with no name attribute (more tricky to mock with simple class)
        # For now, assume hasattr check handles it.
        # vm_no_name = object() # Lacks 'name'
        # self.assertIsNone(self.constraint_manager.get_preferred_host_for_vm(vm_no_name))


if __name__ == '__main__':
    unittest.main()
