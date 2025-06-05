import unittest
from unittest.mock import MagicMock, patch
from modules.load_evaluator import LoadEvaluator

class TestLoadEvaluator(unittest.TestCase):
    def setUp(self):
        # Sample host data (list of dictionaries, as expected by LoadEvaluator)
        self.mock_hosts_data = [
            {'name': 'host1', 'cpu_usage': 50, 'cpu_capacity': 100, 'memory_usage': 20, 'memory_capacity': 100, 'disk_io_usage': 10, 'disk_io_capacity': 100, 'network_io_usage': 5, 'network_capacity': 100},
            {'name': 'host2', 'cpu_usage': 70, 'cpu_capacity': 100, 'memory_usage': 30, 'memory_capacity': 100, 'disk_io_usage': 15, 'disk_io_capacity': 100, 'network_io_usage': 10, 'network_capacity': 100},
            {'name': 'host3', 'cpu_usage': 20, 'cpu_capacity': 100, 'memory_usage': 50, 'memory_capacity': 100, 'disk_io_usage': 5, 'disk_io_capacity': 100, 'network_io_usage': 20, 'network_capacity': 100},
        ]
        self.load_evaluator = LoadEvaluator(hosts=self.mock_hosts_data)

    def test_get_all_host_resource_percentages_map_basic(self):
        """Test get_all_host_resource_percentages_map with valid host data."""
        expected_map = {
            'host1': {'cpu': 50.0, 'memory': 20.0, 'disk': 10.0, 'network': 5.0},
            'host2': {'cpu': 70.0, 'memory': 30.0, 'disk': 15.0, 'network': 10.0},
            'host3': {'cpu': 20.0, 'memory': 50.0, 'disk': 5.0, 'network': 20.0},
        }
        result_map = self.load_evaluator.get_all_host_resource_percentages_map()
        self.assertEqual(result_map, expected_map)

    def test_get_all_host_resource_percentages_map_empty_hosts(self):
        """Test get_all_host_resource_percentages_map with an empty list of hosts."""
        evaluator_empty = LoadEvaluator(hosts=[])
        self.assertEqual(evaluator_empty.get_all_host_resource_percentages_map(), {})

    def test_get_all_host_resource_percentages_map_missing_name(self):
        """Test with hosts missing 'name' (should use placeholders)."""
        hosts_missing_name = [
            {'cpu_usage': 50, 'cpu_capacity': 100, 'memory_usage': 20, 'memory_capacity': 100, 'disk_io_usage':10, 'disk_io_capacity':100, 'network_io_usage':5, 'network_capacity':100}, # No name
            {'name': 'hostB', 'cpu_usage': 70, 'cpu_capacity': 100, 'memory_usage': 30, 'memory_capacity': 100, 'disk_io_usage':15, 'disk_io_capacity':100, 'network_io_usage':10, 'network_capacity':100},
        ]
        evaluator = LoadEvaluator(hosts=hosts_missing_name)
        expected_map = {
            'unknown_host_0': {'cpu': 50.0, 'memory': 20.0, 'disk': 10.0, 'network': 5.0},
            'hostB': {'cpu': 70.0, 'memory': 30.0, 'disk': 15.0, 'network': 10.0},
        }
        result_map = evaluator.get_all_host_resource_percentages_map()
        self.assertEqual(result_map, expected_map)

    def test_get_resource_percentage_lists_host_not_dict(self):
        """Test get_resource_percentage_lists when a host_data is not a dict."""
        hosts_with_invalid_entry = [
            {'name': 'hostA', 'cpu_usage': 50, 'cpu_capacity': 100, 'memory_usage': 20, 'memory_capacity': 100, 'disk_io_usage':10, 'disk_io_capacity':100, 'network_io_usage':5, 'network_capacity':100},
            "not_a_dict_host_entry", # Invalid entry
            {'name': 'hostC', 'cpu_usage': 70, 'cpu_capacity': 100, 'memory_usage': 30, 'memory_capacity': 100, 'disk_io_usage':15, 'disk_io_capacity':100, 'network_io_usage':10, 'network_capacity':100},
        ]
        evaluator = LoadEvaluator(hosts=hosts_with_invalid_entry)
        cpu_p, mem_p, disk_p, net_p = evaluator.get_resource_percentage_lists()

        self.assertEqual(cpu_p, [50.0, 0.0, 70.0]) # 0.0 for the invalid entry
        self.assertEqual(mem_p, [20.0, 0.0, 30.0])
        # This also tests that get_all_host_resource_percentages_map handles this padding if names are generated

        # And check the map generation with such data
        result_map = evaluator.get_all_host_resource_percentages_map()
        expected_map_with_invalid = {
            'hostA': {'cpu': 50.0, 'memory': 20.0, 'disk': 10.0, 'network': 5.0},
            'invalid_host_data_1': {'cpu': 0.0, 'memory': 0.0, 'disk': 0.0, 'network': 0.0},
            'hostC': {'cpu': 70.0, 'memory': 30.0, 'disk': 15.0, 'network': 10.0},
        }
        self.assertEqual(result_map, expected_map_with_invalid)


    def test_evaluate_imbalance_with_overrides(self):
        """Test evaluate_imbalance when override percentage lists are provided."""
        # These overrides should lead to CPU imbalance, but memory balance.
        cpu_override = [80.0, 20.0, 30.0] # Diff = 60
        mem_override = [50.0, 55.0, 60.0] # Diff = 10
        # Use original disk/net from self.load_evaluator's setup for simplicity in this test
        _, _, disk_orig, net_orig = self.load_evaluator.get_resource_percentage_lists()

        # Default threshold is 15% for aggressiveness 3
        imbalance_details = self.load_evaluator.evaluate_imbalance(
            metrics_to_check=['cpu', 'memory'],
            aggressiveness=3,
            cpu_percentages_override=cpu_override,
            mem_percentages_override=mem_override,
            disk_percentages_override=disk_orig, # Pass original for non-focused metrics
            net_percentages_override=net_orig
        )

        self.assertTrue(imbalance_details['cpu']['is_imbalanced'])
        self.assertEqual(imbalance_details['cpu']['current_diff'], 60.0)
        self.assertFalse(imbalance_details['memory']['is_imbalanced'])
        self.assertEqual(imbalance_details['memory']['current_diff'], 10.0)

    def test_evaluate_imbalance_no_overrides(self):
        """Test evaluate_imbalance using internally calculated percentages."""
        # From setUp: host1 CPU 50%, host2 CPU 70%, host3 CPU 20%. Diff = 50.
        # Mem: h1 20%, h2 30%, h3 50%. Diff = 30.
        # Threshold for aggressiveness 3 is 15%. Both should be imbalanced.
        imbalance_details = self.load_evaluator.evaluate_imbalance(
            metrics_to_check=['cpu', 'memory'],
            aggressiveness=3
        )
        self.assertTrue(imbalance_details['cpu']['is_imbalanced'])
        self.assertEqual(imbalance_details['cpu']['current_diff'], 50.0)
        self.assertTrue(imbalance_details['memory']['is_imbalanced'])
        self.assertEqual(imbalance_details['memory']['current_diff'], 30.0)

if __name__ == '__main__':
    unittest.main()
