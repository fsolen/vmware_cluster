import unittest
from modules.load_evaluator import LoadEvaluator
import logging

# Optional: Configure logger for tests if you want to see logs from the module
# logger = logging.getLogger('fdrs')
# logger.setLevel(logging.DEBUG) # Or any desired level
# logger.addHandler(logging.StreamHandler())


class TestLoadEvaluator(unittest.TestCase):

    def setUp(self):
        """
        Set up mock host data for testing LoadEvaluator.
        Network data is adjusted for Scenario 2 to be balanced with aggressiveness=3 (threshold 15%).
        Host1 Net: 2 / 100 = 2%
        Host2 Net: 10 / 100 = 10%
        Difference = 8%, which is < 15% (balanced)
        """
        self.mock_hosts_data = [
            {
                'name': 'host1',
                'cpu_usage': 5000, 'cpu_capacity': 10000,  # CPU 50%
                'memory_usage': 10, 'memory_capacity': 100, # Mem 10%
                'disk_io_usage': 5, 'disk_io_capacity': 100, # Disk 5%
                'network_io_usage': 2, 'network_capacity': 100 # Net 2%
            },
            {
                'name': 'host2',
                'cpu_usage': 1000, 'cpu_capacity': 10000,  # CPU 10%
                'memory_usage': 80, 'memory_capacity': 100, # Mem 80%
                'disk_io_usage': 70, 'disk_io_capacity': 100, # Disk 70%
                'network_io_usage': 10, 'network_capacity': 100 # Net 10%
            }
        ]
        self.evaluator = LoadEvaluator(self.mock_hosts_data)
        self.aggressiveness = 3 # Corresponds to a 15% threshold in LoadEvaluator

    def test_evaluate_imbalance_scenarios(self):
        # --- Scenario 1: metrics_to_check with a specific list ['cpu', 'disk'] ---
        result_scenario1 = self.evaluator.evaluate_imbalance(
            metrics_to_check=['cpu', 'disk'],
            aggressiveness=self.aggressiveness
        )

        # Assertions for Scenario 1
        self.assertIn('cpu', result_scenario1)
        self.assertIn('disk', result_scenario1)
        self.assertEqual(len(result_scenario1.keys()), 2, "Should only contain 'cpu' and 'disk'")

        # CPU: Host1 50%, Host2 10%. Diff = 40%. Threshold for aggressiveness 3 is 15%. 40% > 15% -> Imbalanced.
        self.assertTrue(result_scenario1['cpu']['is_imbalanced'])
        # Disk: Host1 5%, Host2 70%. Diff = 65%. Threshold 15%. 65% > 15% -> Imbalanced.
        self.assertTrue(result_scenario1['disk']['is_imbalanced'])

        # --- Scenario 2: metrics_to_check is None (should use all default metrics) ---
        result_scenario2 = self.evaluator.evaluate_imbalance(
            metrics_to_check=None,
            aggressiveness=self.aggressiveness
        )

        # Assertions for Scenario 2
        self.assertIn('cpu', result_scenario2)
        self.assertIn('memory', result_scenario2)
        self.assertIn('disk', result_scenario2)
        self.assertIn('network', result_scenario2)
        self.assertEqual(len(result_scenario2.keys()), 4, "Should contain all four default metrics")

        # CPU: Still imbalanced (40% > 15%)
        self.assertTrue(result_scenario2['cpu']['is_imbalanced'])
        # Memory: Host1 10%, Host2 80%. Diff = 70%. 70% > 15% -> Imbalanced.
        self.assertTrue(result_scenario2['memory']['is_imbalanced'])
        # Disk: Still imbalanced (65% > 15%)
        self.assertTrue(result_scenario2['disk']['is_imbalanced'])
        # Network: Host1 2%, Host2 10%. Diff = 8%. 8% < 15% -> Balanced.
        self.assertFalse(result_scenario2['network']['is_imbalanced'])

        # --- Scenario 3: metrics_to_check is an empty list [] ---
        result_scenario3 = self.evaluator.evaluate_imbalance(
            metrics_to_check=[],
            aggressiveness=self.aggressiveness
        )

        # Assertions for Scenario 3
        self.assertEqual(len(result_scenario3.keys()), 0, "Result should be an empty dictionary for empty metrics_to_check list")
        self.assertEqual(result_scenario3, {})


if __name__ == '__main__':
    unittest.main()
