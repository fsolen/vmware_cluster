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

    def test_get_preferred_host_for_vm_excludes_source_host(self):
        """
        Test that get_preferred_host_for_vm does not recommend the source host,
        even if it might seem like a valid option to its helper methods without explicit exclusion.
        This test relies on the internal helpers correctly excluding the source host.
        """
        vm_to_move = MockVM(name="testVM01")
        source_host = MockHost(name="sourceHost")
        target_host = MockHost(name="targetHost")

        vm_to_move.host = source_host # vm_to_move is on sourceHost

        self.mock_cluster_state.hosts = [source_host, target_host]
        self.mock_cluster_state.get_host_of_vm.return_value = source_host

        # VM distribution setup
        self.constraint_manager.vm_distribution = {
            "testVM": [vm_to_move, MockVM(name="testVM02")] # vm_to_move is part of group "testVM"
        }
        # self.mock_cluster_state.vms needs to be set if enforce_anti_affinity is called internally
        self.mock_cluster_state.vms = self.constraint_manager.vm_distribution["testVM"]


        # Scenario: Moving to targetHost is a perfect balance move.
        # SourceHost has 2 VMs of group "testVM", targetHost has 0.
        # If vm_to_move is moved from sourceHost (now 1) to targetHost (now 1), counts are (1,1) -> perfect.
        # What if sourceHost was the only one that could make it "perfect" (e.g. by reducing its count)?
        # The helpers _find_perfect_balance_host and _find_better_than_source_host
        # are already confirmed (or were intended to be confirmed in previous step)
        # to exclude source_host_name from their list of candidates.
        # This test ensures get_preferred_host_for_vm, through these helpers, achieves this.

        # Let's make sourceHost appear as the only "perfect" option if it *were* allowed.
        # Suppose current counts are: sourceHost: 1 (vm_to_move), targetHost: 1. (Total 2 VMs in group)
        # If vm_to_move is "moved" from sourceHost (becomes 0) to sourceHost (becomes 1) -> (1,1) - perfect. (This is non-sensical)
        # If vm_to_move is "moved" from sourceHost (becomes 0) to targetHost (becomes 2) -> (0,2) - not perfect.
        # This setup forces the helpers to be smart.

        # Redefine active_hosts for this specific test context if setUp's version is too broad
        self.constraint_manager.active_hosts = [source_host, target_host] # Ensure this is used if CM uses it directly

        # Mock current_host_group_counts that will be calculated inside get_preferred_host_for_vm
        # To do this accurately, we need to control what get_host_of_vm returns for *all* VMs in the group.
        vm_group_partner = MockVM(name="testVM02") # The other VM in the group

        # Setup: vm_to_move on sourceHost, vm_group_partner on targetHost.
        # So, current_host_group_counts will be {'sourceHost': 1, 'targetHost': 1}
        # This is already perfectly balanced.
        # If vm_to_move is considered (from sourceHost),
        #   - moving to targetHost: sourceHost=0, targetHost=2. Max-Min=2. Not perfect.
        #   - "moving" to sourceHost: sourceHost=1, targetHost=1. Max-Min=0. Perfect. (But should be excluded)

        # Let's adjust the scenario to make it more explicit for exclusion:
        # vm_to_move on sourceHost. vm_group_partner also on sourceHost.
        # current_host_group_counts: {'sourceHost': 2, 'targetHost': 0}
        # vm_to_move is testVM01.
        # If vm_to_move (testVM01) moves from sourceHost (becomes 1) to targetHost (becomes 1): {'sourceHost':1, 'targetHost':1}. Perfect.
        # If vm_to_move (testVM01) "moves" from sourceHost (becomes 1) to sourceHost (becomes 1): {'sourceHost':1, 'targetHost':0}. Not perfect.

        # The key is that the internal `_find_perfect_balance_host` and `_find_better_than_source_host`
        # iterate `active_hosts`. If `target_host_name == source_host_name`, they `continue`.
        # So, sourceHost should never be returned.

        self.mock_cluster_state.get_host_of_vm.side_effect = lambda vm: \
            source_host if vm == vm_to_move or vm == vm_group_partner else None

        # Ensure vm_distribution correctly reflects vm_group_partner
        self.constraint_manager.vm_distribution["testVM"] = [vm_to_move, vm_group_partner]
        self.mock_cluster_state.vms = [vm_to_move, vm_group_partner]


        # Call the method under test
        preferred_host = self.constraint_manager.get_preferred_host_for_vm(vm_to_move)

        # Assertion: The preferred host should be target_host, not source_host.
        # If target_host is the only valid option, it should be chosen.
        # If no valid option other than source_host, None should be returned.
        self.assertIsNotNone(preferred_host, "A target host should have been found.")
        self.assertEqual(preferred_host.name, target_host.name,
                         "Preferred host should be the targetHost, not the sourceHost.")
        self.assertNotEqual(preferred_host.name, source_host.name,
                            "Preferred host must not be the source host.")


if __name__ == '__main__':
    unittest.main()
