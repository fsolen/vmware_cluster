from modules.logger import Logger

logger = Logger()

class LoadEvaluator:
    def __init__(self, cluster_state):
        self.cluster_state = cluster_state

    def evaluate_imbalance(self, aggressiveness=3):
        """
        Evaluate imbalance in resource distribution based on aggressiveness level.
        """
        logger.info(f"Evaluating load imbalance with aggressiveness level {aggressiveness}...")
        # Dummy logic: check if there is more than a 20% difference in VM count between clusters
        cluster_counts = [len(vms) for vms in self.cluster_state.values()]
        max_vms = max(cluster_counts)
        min_vms = min(cluster_counts)

        if (max_vms - min_vms) > 0.2 * max_vms:
            logger.warning("Imbalance detected!")
            return True
        return False

