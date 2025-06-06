import unittest
from unittest.mock import MagicMock, patch, call # Import call for checking call order
from modules.migration_planner import MigrationManager
from modules.constraint_manager import ConstraintManager # For type hinting if needed, or direct use
from modules.load_evaluator import LoadEvaluator # For type hinting or direct use

# Basic Host and VM object structures for mocking (can be shared if in a common test utils)
class MockHost:
    def __init__(self, name, vms=None):
        self.name = name
        self.vms = vms if vms else []

class MockVM:
    def __init__(self, name, host=None, config=None, metrics=None):
        self.name = name
        self.host = host # This would be a MockHost object
        self.config = config or MagicMock()
        self.metrics = metrics or {} # e.g. {'cpu_usage_abs': 1000, 'memory_usage_abs': 2048}

class TestMigrationPlanner(unittest.TestCase):
    def setUp(self):
        self.mock_cluster_state = MagicMock()
        self.mock_constraint_mgr = MagicMock(spec=ConstraintManager)
        self.mock_load_evaluator = MagicMock(spec=LoadEvaluator)

        self.planner = MigrationManager(
            cluster_state=self.mock_cluster_state,
            constraint_manager=self.mock_constraint_mgr,
            load_evaluator=self.mock_load_evaluator,
            aggressiveness=3,
            max_total_migrations=5 # Keep this small for testing limits
        )

        # Define some hosts and VMs for use in tests
        self.host1 = MockHost(name="host1")
        self.host2 = MockHost(name="host2")
        self.host3 = MockHost(name="host3")
        self.mock_cluster_state.hosts = [self.host1, self.host2, self.host3]

        # Mock load evaluator's hosts to be consistent for ordered list generation
        self.mock_load_evaluator.hosts = self.mock_cluster_state.hosts


        self.vm1 = MockVM(name="vm1", host=self.host1, metrics={'cpu_usage_abs': 100, 'memory_usage_abs': 1024})
        self.vm2 = MockVM(name="vm2", host=self.host1, metrics={'cpu_usage_abs': 200, 'memory_usage_abs': 2048})
        self.vm3 = MockVM(name="vm3", host=self.host2, metrics={'cpu_usage_abs': 150, 'memory_usage_abs': 1536})
        self.mock_cluster_state.vms = [self.vm1, self.vm2, self.vm3]

        # Mock cluster_state methods that might be used
        self.mock_cluster_state.get_host_of_vm.side_effect = lambda vm_obj: vm_obj.host

        # Mock host_metrics and vm_metrics in cluster_state
        # Ensure 'memory_usage' key holds the host's absolute total memory usage,
        # consistent with ClusterState.annotate_hosts_with_metrics.
        self.mock_cluster_state.host_metrics = {
            "host1": {'cpu_usage': 300, 'cpu_capacity': 1000, 'memory_usage': 3072, 'memory_capacity': 8192, 'disk_io_usage': 0, 'disk_io_capacity': 1, 'network_io_usage': 0, 'network_capacity': 1},
            "host2": {'cpu_usage': 150, 'cpu_capacity': 1000, 'memory_usage': 1536, 'memory_capacity': 8192, 'disk_io_usage': 0, 'disk_io_capacity': 1, 'network_io_usage': 0, 'network_capacity': 1},
            "host3": {'cpu_usage': 50,  'cpu_capacity': 1000, 'memory_usage': 1024, 'memory_capacity': 8192, 'disk_io_usage': 0, 'disk_io_capacity': 1, 'network_io_usage': 0, 'network_capacity': 1},
        }
        self.mock_cluster_state.vm_metrics = {
            "vm1": self.vm1.metrics,
            "vm2": self.vm2.metrics,
            "vm3": self.vm3.metrics,
        }

        # Mock LoadEvaluator's get_resource_percentage_lists to return some initial values
        self.initial_cpu_p = [30.0, 15.0, 5.0] # Corresponds to host_metrics above
        self.initial_mem_p = [37.5, 18.75, 12.5]
        self.initial_disk_p = [0.0, 0.0, 0.0]
        self.initial_net_p = [0.0, 0.0, 0.0]
        self.mock_load_evaluator.get_resource_percentage_lists.return_value = (
            self.initial_cpu_p, self.initial_mem_p, self.initial_disk_p, self.initial_net_p
        )
        self.initial_host_map = {
            "host1": {'cpu': 30.0, 'memory': 37.5, 'disk':0,'network':0},
            "host2": {'cpu': 15.0, 'memory': 18.75, 'disk':0,'network':0},
            "host3": {'cpu': 5.0,  'memory': 12.5, 'disk':0,'network':0},
        }
        self.mock_load_evaluator.get_all_host_resource_percentages_map.return_value = self.initial_host_map

    def test_max_total_migrations_initialization(self):
        """Test the initialization of max_total_migrations in MigrationManager."""
        # Scenario 1: max_total_migrations is None
        planner_none = MigrationManager(
            self.mock_cluster_state,
            self.mock_constraint_mgr,
            self.mock_load_evaluator,
            max_total_migrations=None
        )
        self.assertEqual(planner_none.max_total_migrations, 20) # Internal default

        # Scenario 2: max_total_migrations is a specific integer
        planner_specific = MigrationManager(
            self.mock_cluster_state,
            self.mock_constraint_mgr,
            self.mock_load_evaluator,
            max_total_migrations=50
        )
        self.assertEqual(planner_specific.max_total_migrations, 50)

        # Scenario 3: max_total_migrations argument is omitted (uses __init__ signature default)
        planner_default_sig = MigrationManager(
            self.mock_cluster_state,
            self.mock_constraint_mgr,
            self.mock_load_evaluator
        )
        self.assertEqual(planner_default_sig.max_total_migrations, 20) # Signature default

        # Scenario 4: max_total_migrations is a string number (should be cast to int)
        planner_string = MigrationManager(
            self.mock_cluster_state,
            self.mock_constraint_mgr,
            self.mock_load_evaluator,
            max_total_migrations="15"
        )
        self.assertEqual(planner_string.max_total_migrations, 15)


    def test_get_simulated_load_data_after_migrations_correctly_updates_targets(self): # Renamed and enhanced
        """
        Test the simulation of migrations on load data, ensuring all targets are updated
        and lists/maps are consistent. Reflects new method name and signature.
        """
        # vm1 (100 CPU, 1024 MEM) from host1 (initial on host1)
        # vm3 (150 CPU, 1536 MEM) from host2 (initial on host2)
        migrations_to_simulate = [
            {'vm': self.vm1, 'target_host': self.host2}, # vm1 from host1 to host2
            {'vm': self.vm3, 'target_host': self.host3}  # vm3 from host2 to host3
        ]

        # Initial absolute states from setUp's self.mock_cluster_state.host_metrics:
        # host1: cpu_usage 300, mem_usage_abs 3072 (overallMemoryUsage from ClusterState change)
        # host2: cpu_usage 150, mem_usage_abs 1536
        # host3: cpu_usage 50,  mem_usage_abs 1024
        # Capacities: CPU 1000, Mem 8192 for all.

        # VM metrics from setUp:
        # vm1: cpu_usage_abs 100, memory_usage_abs 1024
        # vm3: cpu_usage_abs 150, memory_usage_abs 1536

        # Simulation Step 1: vm1 (100C, 1024M) moves from host1 to host2
        # host1: cpu_usage_abs = 300-100=200, mem_usage_abs = 3072-1024=2048
        # host2: cpu_usage_abs = 150+100=250, mem_usage_abs = 1536+1024=2560
        # host3: no change yet (cpu 50, mem 1024)

        # Simulation Step 2: vm3 (150C, 1536M) moves from host2 to host3
        # host1: no change (cpu 200, mem 2048)
        # host2: cpu_usage_abs = 250-150=100, mem_usage_abs = 2560-1536=1024
        # host3: cpu_usage_abs = 50+150=200,  mem_usage_abs = 1024+1536=2560

        # Expected final absolute usages for CPU/Memory:
        # host1: CPU 200, Mem 2048
        # host2: CPU 100, Mem 1024
        # host3: CPU 200, Mem 2560

        # Expected percentages (capacities: CPU 1000, Mem 8192 for all from setUp):
        # host1: CPU 200/1000 = 20.0%, Mem 2048/8192 = 25.0%
        # host2: CPU 100/1000 = 10.0%, Mem 1024/8192 = 12.5%
        # host3: CPU 200/1000 = 20.0%, Mem 2560/8192 = 31.25%

        # self.mock_load_evaluator.hosts is [self.host1, self.host2, self.host3] in setUp.
        # This order is used by _get_simulated_load_data_after_migrations to create lists.
        expected_cpu_p = [20.0, 10.0, 20.0]
        expected_mem_p = [25.0, 12.5, 31.25]

        # Call the method with new signature (no initial_host_load_map)
        sim_cpu, sim_mem, sim_disk, sim_net, sim_map = \
            self.planner._get_simulated_load_data_after_migrations(migrations_to_simulate)

        self.assertEqual(sim_cpu, expected_cpu_p, "Simulated CPU percentages list is incorrect.")
        self.assertEqual(sim_mem, expected_mem_p, "Simulated Memory percentages list is incorrect.")

        # Disk and Network percentages should be the original ones, passed through
        # self.initial_disk_p and self.initial_net_p are set in setUp from get_resource_percentage_lists
        self.assertEqual(sim_disk, self.initial_disk_p, "Disk percentages should be passed through.")
        self.assertEqual(sim_net, self.initial_net_p, "Network percentages should be passed through.")

        # Verify the map contents for all hosts
        # The order of hosts in self.initial_disk_p corresponds to host1, host2, host3
        self.assertEqual(sim_map.get("host1"), {'cpu': 20.0, 'memory': 25.0, 'disk': self.initial_disk_p[0], 'network': self.initial_net_p[0]})
        self.assertEqual(sim_map.get("host2"), {'cpu': 10.0, 'memory': 12.5, 'disk': self.initial_disk_p[1], 'network': self.initial_net_p[1]})
        self.assertEqual(sim_map.get("host3"), {'cpu': 20.0, 'memory': 31.25, 'disk': self.initial_disk_p[2], 'network': self.initial_net_p[2]})

    def test_plan_anti_affinity_migrations_iterative_calls(self):
        """
        Test _plan_anti_affinity_migrations to ensure it iteratively calls
        get_preferred_host_for_vm with accumulated planned migrations.
        """
        # VMs from the same anti-affinity group "aaVM"
        vm_aa1 = MockVM(name="aaVM01", host=self.host1)
        vm_aa1.config = MagicMock(template=False) # Ensure not treated as template
        vm_aa2 = MockVM(name="aaVM02", host=self.host1) # Also on host1, causing violation
        vm_aa2.config = MagicMock(template=False) # Ensure not treated as template
        vm_aa3 = MockVM(name="aaVM03", host=self.host2) # On host2, for balancing counts
        vm_aa3.config = MagicMock(template=False)


        # Mock constraint_manager setup
        self.mock_constraint_mgr.violations = [vm_aa1, vm_aa2] # Two VMs from same group need moving
        # Ensure vm_distribution is populated if enforce_anti_affinity is called
        self.mock_constraint_mgr.vm_distribution = {
            "aaVM": [vm_aa1, vm_aa2, vm_aa3]
        }
        # Ensure that if calculate_anti_affinity_violations is called, it returns the violations we want for the test.
        # And also set the .violations attribute directly in case the method attempts to use a pre-calculated one.
        self.mock_constraint_mgr.violations = [vm_aa1, vm_aa2]
        self.mock_constraint_mgr.calculate_anti_affinity_violations.return_value = [vm_aa1, vm_aa2]

        # Mock get_preferred_host_for_vm:
        # 1st call (for vm_aa1): return host2, no prior planned migrations
        # 2nd call (for vm_aa2): return host3, expect vm_aa1's move to host2 in planned_migrations

        # Define side effect function for get_preferred_host_for_vm
        def get_preferred_host_side_effect(vm_obj, planned_migrations_this_cycle=None):
            if vm_obj == vm_aa1:
                # For vm_aa1, no migrations should be planned yet in this cycle
                self.assertEqual(len(planned_migrations_this_cycle or []), 0)
                return self.host2 # Plan to move vm_aa1 to host2
            elif vm_obj == vm_aa2:
                # For vm_aa2, vm_aa1's migration to host2 should be in planned_migrations_this_cycle
                self.assertIsNotNone(planned_migrations_this_cycle)
                self.assertEqual(len(planned_migrations_this_cycle), 1)
                self.assertEqual(planned_migrations_this_cycle[0]['vm'], vm_aa1)
                self.assertEqual(planned_migrations_this_cycle[0]['target_host'], self.host2)
                return self.host3 # Plan to move vm_aa2 to host3
            # Adding a fail condition to see if it's called with unexpected VMs or None is returned implicitly
            self.fail(f"get_preferred_host_side_effect called with unexpected vm_obj: {getattr(vm_obj, 'name', 'UnknownVM')}")
            return None # Should not be reached

        self.mock_constraint_mgr.get_preferred_host_for_vm.side_effect = get_preferred_host_side_effect

        # Mock _would_fit_on_host to always return True for simplicity
        self.planner._would_fit_on_host = MagicMock(return_value=True)

        # Mock cluster_state.get_host_of_vm for current host info
        self.mock_cluster_state.get_host_of_vm.side_effect = lambda vm: vm.host

        vms_already_in_overall_plan = set()
        planned_aa_migrations = self.planner._plan_anti_affinity_migrations(vms_already_in_overall_plan)

        self.assertEqual(len(planned_aa_migrations), 2)
        self.assertEqual(planned_aa_migrations[0]['vm'], vm_aa1)
        self.assertEqual(planned_aa_migrations[0]['target_host'], self.host2)
        self.assertEqual(planned_aa_migrations[1]['vm'], vm_aa2)
        self.assertEqual(planned_aa_migrations[1]['target_host'], self.host3)

        self.assertEqual(self.mock_constraint_mgr.get_preferred_host_for_vm.call_count, 2)
        # The assertions for the content of planned_migrations_this_cycle are done
        # inside the get_preferred_host_side_effect.
        # assert_has_calls will see the *final* state of mutable arguments if not careful.
        # We can use ANY here for planned_migrations_this_cycle as its state at call time
        # is verified by the side_effect.
        expected_calls = [
            call(vm_aa1, planned_migrations_this_cycle=unittest.mock.ANY),
            call(vm_aa2, planned_migrations_this_cycle=unittest.mock.ANY)
        ]
        self.mock_constraint_mgr.get_preferred_host_for_vm.assert_has_calls(expected_calls, any_order=False)
        self.assertIn(vm_aa1.name, vms_already_in_overall_plan)
        self.assertIn(vm_aa2.name, vms_already_in_overall_plan)


    @patch('modules.migration_planner.MigrationManager._get_simulated_load_data_after_migrations') # Updated patch name
    def test_plan_migrations_iterative_path_with_aa_moves(
        self, mock_get_simulated_load_data # Updated mock name
    ):
        """Test plan_migrations when AA moves occur, triggering simulation."""
        # Mock AA planner to return one migration
        aa_migration = {'vm': self.vm1, 'target_host': self.host2, 'reason': 'Anti-Affinity'}
        self.planner._plan_anti_affinity_migrations = MagicMock(return_value=[aa_migration])

        # Mock simulation results
        sim_cpu_p = [20.0, 25.0, 5.0]
        sim_mem_p = [25.0, 31.25, 12.5]
        sim_disk_p = self.initial_disk_p
        sim_net_p = self.initial_net_p
        sim_map = {"host1": {'cpu': 20.0, 'memory': 25.0, 'disk':0,'network':0},
                   "host2": {'cpu': 25.0, 'memory': 31.25, 'disk':0,'network':0},
                   "host3": {'cpu': 5.0, 'memory': 12.5, 'disk':0,'network':0}}
        mock_get_simulated_load_data.return_value = (sim_cpu_p, sim_mem_p, sim_disk_p, sim_net_p, sim_map) # Updated mock name

        # Mock load evaluator to be called with simulated data
        # Ensure max_usage aligns with sim_cpu_p for host2 to be selected as source
        mock_simulated_imbalance_details = {'cpu': {'is_imbalanced': True, 'max_usage': 25.0}}
        self.mock_load_evaluator.evaluate_imbalance.return_value = mock_simulated_imbalance_details # Configure the existing mock

        # DO NOT mock self.planner._plan_balancing_migrations here, we want to test its interaction
        # Spy on _select_vms_to_move and _find_better_host_for_balancing if needed, or ensure they return empty/valid
        # For this test, assume _plan_balancing_migrations will call evaluate_imbalance.
        # To ensure _plan_balancing_migrations doesn't do too much else, we can mock its internal calls if they are complex.
        # For now, let it run, but ensure it produces some known output or no output for balancing.
        # Let's make it return a known balancing migration to check the final plan.
        # To do this without mocking _plan_balancing_migrations itself, we need to mock its dependencies.
        self.planner._select_vms_to_move = MagicMock(return_value=[self.vm3]) # Assume vm3 is selected
        self.planner._find_better_host_for_balancing = MagicMock(return_value=self.host3) # Assume host3 is found for vm3

        # If _plan_balancing_migrations is complex, this test becomes an integration test for it.
        # The alternative is to have a separate, more focused test for _plan_balancing_migrations's internals.

        final_plan = self.planner.plan_migrations()

        self.planner._plan_anti_affinity_migrations.assert_called_once()
        mock_get_simulated_load_data.assert_called_once_with([aa_migration]) # Call signature updated

        # Check that evaluate_imbalance was called with simulated overrides
        self.mock_load_evaluator.evaluate_imbalance.assert_called_once_with(
            aggressiveness=self.planner.aggressiveness,
            cpu_percentages_override=sim_cpu_p,
            mem_percentages_override=sim_mem_p,
            disk_percentages_override=sim_disk_p,
            net_percentages_override=sim_net_p
        )
        # We are no longer mocking _plan_balancing_migrations, so we cannot assert its call directly here.
        # Instead, we verified that evaluate_imbalance was called correctly by it.
        # And we check the final output based on the mocks for _select_vms_to_move and _find_better_host_for_balancing.
        self.assertEqual(len(final_plan), 2) # vm1 from AA, vm3 from balancing
        self.assertIn((self.vm1, self.host2), final_plan)
        self.assertIn((self.vm3, self.host3), final_plan)


    def test_plan_migrations_no_aa_moves_direct_to_balancing(self):
        """Test plan_migrations when no AA moves, uses initial load for balancing."""
        self.planner._plan_anti_affinity_migrations = MagicMock(return_value=[]) # No AA moves

        # Mock simulation (should not be called directly as no AA migs)
        self.planner._get_simulated_load_data_after_migrations = MagicMock() # Updated mock name

        mock_initial_imbalance_details = {'cpu': {'is_imbalanced': False}} # Example
        self.mock_load_evaluator.evaluate_imbalance.return_value = mock_initial_imbalance_details # Configure existing mock

        # DO NOT mock _plan_balancing_migrations. Let it run.
        # If we expect no balancing moves because imbalance is false, then mock _select_vms_to_move to return []
        self.planner._select_vms_to_move = MagicMock(return_value=[])
        # _find_better_host_for_balancing won't be called if no VMs are selected.

        self.planner.plan_migrations()

        self.planner._plan_anti_affinity_migrations.assert_called_once()
        self.planner._get_simulated_load_data_after_migrations.assert_not_called() # Updated mock name

        # evaluate_imbalance called by _plan_balancing_migrations, with no overrides
        self.mock_load_evaluator.evaluate_imbalance.assert_called_once_with(
            aggressiveness=self.planner.aggressiveness,
            cpu_percentages_override=None, # Important: No overrides
            mem_percentages_override=None,
            disk_percentages_override=None,
            net_percentages_override=None
        )
        # We are no longer mocking _plan_balancing_migrations, so cannot assert its call directly.
        # Its effect (calling evaluate_imbalance) is checked above.
        # And _select_vms_to_move returning [] means no balancing migrations will be added.

    def test_migration_limiting(self):
        """Test that plan_migrations respects max_total_migrations."""
        self.planner.max_total_migrations = 2 # Limit to 2 migrations

        # AA proposes 1 move
        aa_mig = [{'vm': self.vm1, 'target_host': self.host2, 'reason': 'Anti-Affinity'}]
        self.planner._plan_anti_affinity_migrations = MagicMock(return_value=aa_mig)

        # Balancing proposes 2 moves. To achieve this without mocking _plan_balancing_migrations,
        # we need to set up its dependencies:
        # 1. evaluate_imbalance should indicate imbalance.
        # 2. _select_vms_to_move should select VMs.
        # 3. _find_better_host_for_balancing should find targets.

        self.mock_load_evaluator.evaluate_imbalance.return_value = {
            'cpu': {'is_imbalanced': True, 'max_diff': 30} # Indicate imbalance
        }
        # _select_vms_to_move will be called multiple times if we want multiple balancing moves.
        # For simplicity, let's assume it's called once and returns two VMs.
        # This might need adjustment if _plan_balancing_migrations iterates hosts.
        # Let's refine: _plan_balancing_migrations iterates hosts.
        # Mock _select_vms_to_move to return one VM for host1, one for host2.

        # To ensure _plan_balancing_migrations actually *can* create bal_mig1 and bal_mig2:
        # It iterates hosts. Let's say host1 is imbalanced.
        # _select_vms_to_move for host1 returns self.vm2
        # _find_better_host_for_balancing for self.vm2 returns self.host3
        # Then, for host2 (if also imbalanced or another logic path)
        # _select_vms_to_move for host2 returns self.vm3
        # _find_better_host_for_balancing for self.vm3 returns self.host1

        # For this test, we simplify: assume _plan_balancing_migrations *would* return these if not for the mock.
        # So, we keep the mock on _plan_balancing_migrations for *this specific test* to isolate limit logic.
        # This means test_migration_limiting tests the truncation logic in plan_migrations,
        # NOT the full interaction that leads to those balancing migrations.
        bal_mig1 = {'vm': self.vm2, 'target_host': self.host3, 'reason': 'Resource Balancing'}
        bal_mig2 = {'vm': self.vm3, 'target_host': self.host1, 'reason': 'Resource Balancing'}
        self.planner._plan_balancing_migrations = MagicMock(return_value=[bal_mig1, bal_mig2])


        # Simulation setup for AA part (not the focus here, but needs to run if AA migs exist)
        self.planner._get_simulated_load_data_after_migrations = MagicMock( # Updated mock name
            return_value=(self.initial_cpu_p, self.initial_mem_p, self.initial_disk_p, self.initial_net_p, self.initial_host_map)
        )
        # evaluate_imbalance inside _plan_balancing_migrations is part of the mock above for this test.

        final_plan_tuples = self.planner.plan_migrations()

        self.assertEqual(len(final_plan_tuples), 2)
        self.assertIn((self.vm1, self.host2), final_plan_tuples) # AA migration should be included
        self.assertIn((self.vm2, self.host3), final_plan_tuples) # First balancing migration
        # vm3 migration should be excluded due to limit

    def test_migration_limiting_only_aa_exceeds(self):
        self.planner.max_total_migrations = 1
        aa_migs = [
            {'vm': self.vm1, 'target_host': self.host2, 'reason': 'Anti-Affinity'},
            {'vm': self.vm2, 'target_host': self.host3, 'reason': 'Anti-Affinity'}
        ]
        self.planner._plan_anti_affinity_migrations = MagicMock(return_value=aa_migs)
        self.planner._plan_balancing_migrations = MagicMock(return_value=[]) # No balancing moves
        self.planner._get_simulated_load_data_after_migrations = MagicMock( # Updated mock name, called if aa_migs not empty
             return_value=(self.initial_cpu_p, self.initial_mem_p, self.initial_disk_p, self.initial_net_p, self.initial_host_map)
        )
        # evaluate_imbalance inside _plan_balancing_migrations might be called if not for empty balancing moves
        # For this test, the focus is on truncation, so an empty return for evaluate_imbalance from balancing is fine
        self.mock_load_evaluator.evaluate_imbalance = MagicMock(return_value={})


        final_plan_tuples = self.planner.plan_migrations()
        self.assertEqual(len(final_plan_tuples), 1)
        self.assertIn((self.vm1, self.host2), final_plan_tuples) # Only the first AA mig

    @patch('modules.migration_planner.logger')
    def test_plan_balancing_migrations_logs_imbalance_details_formatted(self, mock_logger):
        """
        Test that _plan_balancing_migrations logs imbalance_details in the new formatted way.
        """
        sample_imbalance_details = {
            'cpu': {'is_imbalanced': True, 'current_diff': 19.72, 'threshold': 15.0, 'min_usage': 0.37, 'max_usage': 20.09, 'avg_usage': 7.25},
            'memory': {'is_imbalanced': True, 'current_diff': 22.81, 'threshold': 15.0, 'min_usage': 3.23, 'max_usage': 26.04, 'avg_usage': 13.48},
            'disk': {'is_imbalanced': False, 'current_diff': 0.02, 'threshold': 15.0, 'min_usage': 0.0, 'max_usage': 0.02, 'avg_usage': 0.01}
        }
        self.mock_load_evaluator.evaluate_imbalance.return_value = sample_imbalance_details
        
        # Mock cluster_state.hosts to be an empty list to prevent iteration errors if not fully mocked
        # Or ensure it's properly mocked if _plan_balancing_migrations uses it extensively.
        # For this specific logging test, the primary input is imbalance_details.
        # However, _plan_balancing_migrations does iterate over `all_hosts_objects = self.cluster_state.hosts`
        # So, it needs to be a valid iterable, even if empty for this test's focus.
        self.mock_cluster_state.hosts = [] # Minimal setup for this test

        # Call _plan_balancing_migrations
        # Arguments: vms_in_migration_plan, host_resource_percentages_map_for_decision, 
        #            current_planned_migrations_list, sim_cpu_p_override, sim_mem_p_override, 
        #            sim_disk_p_override, sim_net_p_override
        self.planner._plan_balancing_migrations(set(), {}, [], None, None, None, None)

        # Assert logger calls
        log_calls = mock_logger.info.call_args_list

        expected_header_call = call("[MigrationPlanner_Balance] Cluster imbalance details (post-AA sim if any):")
        self.assertIn(expected_header_call, log_calls)

        # Check logs for each resource
        for resource_name, details in sample_imbalance_details.items():
            expected_log = (
                f"  Resource: {resource_name}, Imbalanced: {details.get('is_imbalanced')}, "
                f"Diff: {details.get('current_diff', 0):.2f}%, "
                f"Threshold: {details.get('threshold', 0):.2f}%, "
                f"Min: {details.get('min_usage', 0):.2f}%, "
                f"Max: {details.get('max_usage', 0):.2f}%, "
                f"Avg: {details.get('avg_usage', 0):.2f}%"
            )
            self.assertIn(call(expected_log), log_calls, f"Log for resource {resource_name} not found or incorrect.")
            
        # Total calls: 1 for header + 1 for each resource type (3 in this sample)
        # Plus, if problematic_resources_names is not empty (it is here), one more log.
        # And if problematic_resources_names is empty, one log saying "No specific resource marked as imbalanced".
        # In this case, 'cpu' and 'memory' are imbalanced.
        expected_problematic_log = call(f"[MigrationPlanner_Balance] Problematic resources for balancing (post-AA sim): {['cpu', 'memory']}")
        self.assertIn(expected_problematic_log, log_calls)
        self.assertEqual(mock_logger.info.call_count, 1 + len(sample_imbalance_details) + 1)


if __name__ == '__main__':
    unittest.main()
