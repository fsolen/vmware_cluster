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

        # Pass planned_migrations_this_cycle=None for existing test
        result_host = self.constraint_manager.get_preferred_host_for_vm(vm_to_move, planned_migrations_this_cycle=None)
        mock_find_perfect.assert_called_once()
        mock_find_better.assert_called_once()
        self.assertEqual(result_host, self.host3)

    def test_get_preferred_host_for_vm_initial_checks_vm_invalid(self):
        invalid_vm = MockVM(name="v") # Name too short
        # Pass planned_migrations_this_cycle=None for existing test
        self.assertIsNone(self.constraint_manager.get_preferred_host_for_vm(invalid_vm, planned_migrations_this_cycle=None))

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
        # Pass planned_migrations_this_cycle=None for existing test
        preferred_host = self.constraint_manager.get_preferred_host_for_vm(vm_to_move, planned_migrations_this_cycle=None)

        # Assertion: The preferred host should be target_host, not source_host.
        # If target_host is the only valid option, it should be chosen.
        # If no valid option other than source_host, None should be returned.
        self.assertIsNotNone(preferred_host, "A target host should have been found.")
        self.assertEqual(preferred_host.name, target_host.name,
                         "Preferred host should be the targetHost, not the sourceHost.")
        self.assertNotEqual(preferred_host.name, source_host.name,
                            "Preferred host must not be the source host.")

    def test_get_preferred_host_for_vm_considers_planned_cycle_migrations(self):
        """
        Test that get_preferred_host_for_vm correctly considers planned migrations
        from the current cycle to adjust host group counts.
        """
        # Setup: Group "testvm" with vm_to_process, vm_already_moving
        vm_to_process = MockVM(name="testvm01")
        vm_already_moving = MockVM(name="testvm02")

        hostA = MockHost(name="hostA")
        hostB = MockHost(name="hostB")
        hostC = MockHost(name="hostC")

        # Initial state: vm_to_process and vm_already_moving are on hostA
        vm_to_process.host = hostA
        vm_already_moving.host = hostA

        self.mock_cluster_state.hosts = [hostA, hostB, hostC]

        # Mock get_host_of_vm to reflect initial state for count calculation
        # AND current state for the vm_to_process's source_host_obj
        def get_host_side_effect(vm_obj):
            if vm_obj == vm_to_process: return hostA
            if vm_obj == vm_already_moving: return hostA # Its original host for base count calculation
            return None
        self.mock_cluster_state.get_host_of_vm = MagicMock(side_effect=get_host_side_effect)

        self.constraint_manager.vm_distribution = {
            "testvm": [vm_to_process, vm_already_moving]
        }
        self.mock_cluster_state.vms = [vm_to_process, vm_already_moving] # For enforce_anti_affinity if called

        # Planned migration for vm_already_moving: from hostA to hostB
        planned_migrations = [{'vm': vm_already_moving, 'target_host': hostB}]

        # Expected base_host_group_counts (before considering planned_migrations):
        # hostA: 2 (vm_to_process, vm_already_moving)
        # hostB: 0
        # hostC: 0

        # Expected adjusted_host_group_counts (after vm_already_moving is planned to hostB):
        # hostA: 1 (vm_to_process remains)
        # hostB: 1 (vm_already_moving moves here)
        # hostC: 0

        # Now, we are looking for a host for vm_to_process (currently on hostA).
        # Source host for vm_to_process is hostA. Adjusted count on hostA is 1.
        # Potential targets (excluding sourceHost=hostA): hostB, hostC.
        # Counts for these targets: hostB=1, hostC=0.

        # _find_perfect_balance_host:
        #   If vm_to_process moves from hostA (becomes 0) to hostB (becomes 2): counts (A:0, B:2, C:0). Diff=2. No.
        #   If vm_to_process moves from hostA (becomes 0) to hostC (becomes 1): counts (A:0, B:1, C:1). Diff=1. Yes! hostC is a perfect balance candidate.

        # _find_better_than_source_host:
        #   Adjusted source (hostA) count for vm_to_process's group is 1.
        #   Target hostB count = 1. Not less than source.
        #   Target hostC count = 0. Less than source. hostC is a better candidate.

        # So, hostC should be chosen.

        # We don't need to mock the helper methods themselves, but let the main logic run.
        preferred_host = self.constraint_manager.get_preferred_host_for_vm(
            vm_to_process,
            planned_migrations_this_cycle=planned_migrations
        )

        self.assertIsNotNone(preferred_host, "A preferred host should be found.")
        self.assertEqual(preferred_host.name, "hostC",
                         "hostC should be chosen as it becomes the best option after considering planned migrations.")

    def test_get_preferred_host_for_vm_affinity_grouping(self):
        """
        Test that get_preferred_host_for_vm correctly groups VMs like "vm01" and "vm101"
        under the same prefix "vm" when determining preferred hosts.
        """
        vm_to_move = MockVM(name="vm101") # VM to be moved
        vm_group_mate1 = MockVM(name="vm01")
        vm_group_mate2 = MockVM(name="vm02")

        source_host = MockHost(name="hostA")
        target_host_b = MockHost(name="hostB")
        target_host_c = MockHost(name="hostC")

        # Setup cluster state
        self.mock_cluster_state.hosts = [source_host, target_host_b, target_host_c]
        
        # All VMs are initially on source_host
        self.mock_cluster_state.get_host_of_vm.side_effect = lambda vm_obj: {
            vm_to_move: source_host,
            vm_group_mate1: source_host,
            vm_group_mate2: source_host,
        }.get(vm_obj)

        # Pre-populate vm_distribution. This is key.
        # The prefix for "vm101", "vm01", "vm02" should be "vm".
        self.constraint_manager.vm_distribution = {
            "vm": [vm_to_move, vm_group_mate1, vm_group_mate2]
        }
        # Ensure enforce_anti_affinity is not called and overwriting vm_distribution
        # by also setting self.mock_cluster_state.vms if it were to be called.
        self.mock_cluster_state.vms = [vm_to_move, vm_group_mate1, vm_group_mate2]


        # Current counts for group "vm": hostA=3, hostB=0, hostC=0
        # If vm101 (vm_to_move) moves from hostA (becomes 2) to hostB (becomes 1):
        # Simulated counts: hostA=2, hostB=1, hostC=0. Max-Min = 2. Not perfect.
        # If vm101 (vm_to_move) moves from hostA (becomes 2) to hostC (becomes 1):
        # Simulated counts: hostA=2, hostB=0, hostC=1. Max-Min = 2. Not perfect.
        
        # Let's adjust the scenario for a clearer "perfect" or "better" outcome.
        # Say vm_group_mate2 is on hostB.
        # Initial state: vm101 (hostA), vm01 (hostA), vm02 (hostB)
        # vm_distribution: {'vm': [vm101, vm01, vm02]}
        # get_host_of_vm: vm101->hostA, vm01->hostA, vm02->hostB
        # Base counts for "vm": hostA=2, hostB=1, hostC=0.
        # vm_to_move is vm101 from hostA.
        # If vm101 moves from hostA (becomes 1) to hostC (becomes 1):
        # Simulated: hostA=1, hostB=1, hostC=1. Max-Min = 0. Perfect! -> target_host_c

        self.mock_cluster_state.get_host_of_vm.side_effect = lambda vm_obj: {
            vm_to_move: source_host,
            vm_group_mate1: source_host,
            vm_group_mate2: target_host_b, # This VM is on hostB
        }.get(vm_obj)
        
        self.constraint_manager.vm_distribution = {
            "vm": [vm_to_move, vm_group_mate1, vm_group_mate2]
        }
        self.mock_cluster_state.vms = [vm_to_move, vm_group_mate1, vm_group_mate2]


        # Call the method under test
        preferred_host = self.constraint_manager.get_preferred_host_for_vm(vm_to_move, planned_migrations_this_cycle=None)

        # Assertion:
        # The core of the test is that `get_preferred_host_for_vm` uses "vm" as the prefix.
        # If it used "vm1" or "vm101", it wouldn't find the group in `self.constraint_manager.vm_distribution`
        # and would likely return None or error.
        # A non-None result implies the group was found and processed.
        self.assertIsNotNone(preferred_host, "A preferred host should be found if affinity grouping is correct.")
        
        # Based on the adjusted scenario: hostA (2 initially), hostB (1 initially), hostC (0 initially)
        # vm_to_move is vm101 from hostA.
        # Adjusted counts for source hostA for group "vm" = 2.
        # Potential targets: hostB (count 1), hostC (count 0)
        # Moving vm101 from hostA to hostC:
        #   Simulated counts: hostA=1, hostB=1, hostC=1. Max-Min = 0. This is a perfect balance.
        #   hostC has current count 0, which is the lowest.
        # Moving vm101 from hostA to hostB:
        #   Simulated counts: hostA=1, hostB=2, hostC=0. Max-Min = 2. Not perfect.
        self.assertEqual(preferred_host.name, target_host_c.name,
                         "Preferred host should be hostC for perfect balance based on 'vm' group.")

    @patch('modules.constraint_manager.ConstraintManager._find_better_than_source_host')
    @patch('modules.constraint_manager.ConstraintManager._find_perfect_balance_host')
    def test_get_preferred_host_for_vm_planned_migration_affinity_grouping(self, mock_find_perfect, mock_find_better):
        """
        Tests that planned migrations correctly use rstrip for prefix calculation
        and influence the adjusted_host_group_counts.
        """
        vm_to_move = MockVM(name="appVM101") # Prefix "appVM"
        planned_vm = MockVM(name="appVM01")  # Prefix "appVM"
        other_vm_in_group = MockVM(name="appVM02") # Prefix "appVM"

        hostA = MockHost(name="hostA") # Source for vm_to_move
        hostB = MockHost(name="hostB") # Potential target for vm_to_move
        hostC = MockHost(name="hostC") # Original host of planned_vm
        hostD = MockHost(name="hostD") # Target host for planned_vm

        self.mock_cluster_state.hosts = [hostA, hostB, hostC, hostD]

        # Define initial locations of VMs
        self.mock_cluster_state.get_host_of_vm.side_effect = lambda vm_obj: {
            vm_to_move: hostA,
            planned_vm: hostC,
            other_vm_in_group: hostA,
        }.get(vm_obj)

        # vm_distribution setup: all these VMs belong to group "appVM"
        self.constraint_manager.vm_distribution = {
            "appVM": [vm_to_move, planned_vm, other_vm_in_group]
        }
        # Also provide self.mock_cluster_state.vms for internal enforce_anti_affinity if it gets called
        self.mock_cluster_state.vms = [vm_to_move, planned_vm, other_vm_in_group]

        # Planned migration for planned_vm (appVM01) from hostC to hostD
        planned_migrations = [{'vm': planned_vm, 'target_host': hostD}]

        # Expected base_host_group_counts for "appVM" (before planned_migrations adjustment):
        # hostA: 2 (appVM101, appVM02)
        # hostB: 0
        # hostC: 1 (appVM01)
        # hostD: 0

        # Expected adjusted_host_group_counts for "appVM" (after appVM01 planned C->D):
        # hostA: 2
        # hostB: 0
        # hostC: 0 (appVM01 moved out)
        # hostD: 1 (appVM01 moved in)
        
        # Capture the adjusted_host_group_counts passed to the helper methods
        captured_counts_perfect = {}
        captured_counts_better = {}

        def capture_args_perfect_side_effect(vm, counts, source_host, active_hosts):
            captured_counts_perfect.update(counts)
            # Return a valid host to ensure this path is taken if possible
            # For this test, we want hostB to be the perfectly balanced one after adjustments
            if source_host == hostA.name: # vm_to_move is from hostA
                # Counts after vm_to_move from A to B: A:1, B:1, C:0, D:1 -> Perfect
                if counts.get(hostB.name, 0) == 0 and counts.get(hostA.name,0) == 2: # Based on expected adjusted counts
                     return hostB 
            return None 
        
        def capture_args_better_side_effect(vm, counts, source_host, source_count, active_hosts):
            captured_counts_better.update(counts)
            return None # Fallback, not the primary assertion point for counts

        mock_find_perfect.side_effect = capture_args_perfect_side_effect
        mock_find_better.side_effect = capture_args_better_side_effect
        
        # Call the method under test
        preferred_host = self.constraint_manager.get_preferred_host_for_vm(
            vm_to_move,
            planned_migrations_this_cycle=planned_migrations
        )

        # Determine which captured_counts to use (perfect should be tried first)
        final_captured_counts = captured_counts_perfect if mock_find_perfect.called else captured_counts_better

        # 1. Assert that planned_vm (appVM01) was correctly identified by its prefix "appVM"
        #    and thus adjusted the counts for hostC and hostD.
        self.assertEqual(final_captured_counts.get(hostA.name), 2, "HostA count for appVM group should be 2.")
        self.assertEqual(final_captured_counts.get(hostB.name), 0, "HostB count for appVM group should be 0.")
        self.assertEqual(final_captured_counts.get(hostC.name), 0, "HostC count for appVM group should be 0 after planned move.")
        self.assertEqual(final_captured_counts.get(hostD.name), 1, "HostD count for appVM group should be 1 after planned move.")

        # 2. Assert that the chosen host is correct based on these adjusted counts.
        # vm_to_move (appVM101) is on hostA (adjusted count 2 for group "appVM").
        # Adjusted counts: hostA=2, hostB=0, hostC=0, hostD=1.
        # Target candidates (excluding hostA):
        #   - hostB (count 0): Moving appVM101 A->B => A:1, B:1, C:0, D:1. Min=0,Max=1. Perfect.
        #   - hostC (count 0): Moving appVM101 A->C => A:1, B:0, C:1, D:1. Min=0,Max=1. Perfect.
        #   - hostD (count 1): Moving appVM101 A->D => A:1, B:0, C:0, D:2. Min=0,Max=2. Not perfect.
        # Between hostB and hostC (both count 0, both lead to perfect balance),
        # the one with the lexicographically smaller name is chosen by _find_perfect_balance_host.
        # So, hostB should be chosen.
        self.assertIsNotNone(preferred_host, "A preferred host should have been found.")
        self.assertEqual(preferred_host.name, hostB.name, 
                         "Preferred host should be hostB based on adjusted counts and tie-breaking.")


if __name__ == '__main__':
    unittest.main()
