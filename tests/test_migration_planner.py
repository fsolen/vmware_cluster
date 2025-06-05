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
        self.mock_cluster_state.host_metrics = {
            "host1": {'cpu_usage': 300, 'cpu_capacity': 1000, 'memory_usage_abs': 3072, 'memory_capacity': 8192, 'disk_io_usage': 0, 'disk_io_capacity': 1, 'network_io_usage': 0, 'network_capacity': 1},
            "host2": {'cpu_usage': 150, 'cpu_capacity': 1000, 'memory_usage_abs': 1536, 'memory_capacity': 8192, 'disk_io_usage': 0, 'disk_io_capacity': 1, 'network_io_usage': 0, 'network_capacity': 1},
            "host3": {'cpu_usage': 50,  'cpu_capacity': 1000, 'memory_usage_abs': 1024, 'memory_capacity': 8192, 'disk_io_usage': 0, 'disk_io_capacity': 1, 'network_io_usage': 0, 'network_capacity': 1},
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


    def test_get_simulated_load_lists_after_migrations(self):
        """Test the simulation of migrations on load data."""
        migrations_to_simulate = [
            {'vm': self.vm1, 'target_host': self.host2} # vm1 (100 CPU, 1024 MEM) from host1 to host2
        ]
        # Initial host1: CPU 300/1000 (30%), MEM 3072/8192 (37.5%)
        # Initial host2: CPU 150/1000 (15%), MEM 1536/8192 (18.75%)

        # After vm1 moves from host1 to host2:
        # host1 CPU: 300 - 100 = 200 (20%)
        # host1 MEM: 3072 - 1024 = 2048 (25%)
        # host2 CPU: 150 + 100 = 250 (25%)
        # host2 MEM: 1536 + 1024 = 2560 (31.25%)
        # host3 remains 50 CPU (5%), 1024 MEM (12.5%)

        expected_cpu_p = [20.0, 25.0, 5.0]
        expected_mem_p = [25.0, 31.25, 12.5]

        sim_cpu, sim_mem, sim_disk, sim_net, sim_map = \
            self.planner._get_simulated_load_lists_after_migrations(
                self.initial_host_map, migrations_to_simulate
            )

        self.assertEqual(sim_cpu, expected_cpu_p)
        self.assertEqual(sim_mem, expected_mem_p)
        self.assertEqual(sim_disk, self.initial_disk_p) # Passed through
        self.assertEqual(sim_net, self.initial_net_p)   # Passed through

        expected_sim_map_host1 = {'cpu': 20.0, 'memory': 25.0, 'disk': self.initial_disk_p[0], 'network': self.initial_net_p[0]}
        self.assertEqual(sim_map['host1'], expected_sim_map_host1)
        expected_sim_map_host2 = {'cpu': 25.0, 'memory': 31.25, 'disk': self.initial_disk_p[1], 'network': self.initial_net_p[1]}
        self.assertEqual(sim_map['host2'], expected_sim_map_host2)


    @patch('modules.migration_planner.MigrationManager._get_simulated_load_lists_after_migrations')
    def test_plan_migrations_iterative_path_with_aa_moves(
        self, mock_get_simulated_load
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
                   "host2": {'cpu': 25.0, 'memory': 31.25, 'disk':0,'network':0}, # Simplified, ensure keys exist
                   "host3": {'cpu': 5.0, 'memory': 12.5, 'disk':0,'network':0}}
        mock_get_simulated_load.return_value = (sim_cpu_p, sim_mem_p, sim_disk_p, sim_net_p, sim_map)

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
        mock_get_simulated_load.assert_called_once_with(self.initial_host_map, [aa_migration])

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

        # Mock simulation (should not be called directly, but _plan_balancing_migrations needs its args)
        self.planner._get_simulated_load_lists_after_migrations = MagicMock()

        mock_initial_imbalance_details = {'cpu': {'is_imbalanced': False}} # Example
        self.mock_load_evaluator.evaluate_imbalance.return_value = mock_initial_imbalance_details # Configure existing mock

        # DO NOT mock _plan_balancing_migrations. Let it run.
        # If we expect no balancing moves because imbalance is false, then mock _select_vms_to_move to return []
        self.planner._select_vms_to_move = MagicMock(return_value=[])
        # _find_better_host_for_balancing won't be called if no VMs are selected.

        self.planner.plan_migrations()

        self.planner._plan_anti_affinity_migrations.assert_called_once()
        self.planner._get_simulated_load_lists_after_migrations.assert_not_called()

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
        self.planner._get_simulated_load_lists_after_migrations = MagicMock(
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
        self.planner._get_simulated_load_lists_after_migrations = MagicMock( # Called if aa_migs not empty
             return_value=(self.initial_cpu_p, self.initial_mem_p, self.initial_disk_p, self.initial_net_p, self.initial_host_map)
        )
        self.mock_load_evaluator.evaluate_imbalance = MagicMock(return_value={})


        final_plan_tuples = self.planner.plan_migrations()
        self.assertEqual(len(final_plan_tuples), 1)
        self.assertIn((self.vm1, self.host2), final_plan_tuples) # Only the first AA mig


if __name__ == '__main__':
    unittest.main()
