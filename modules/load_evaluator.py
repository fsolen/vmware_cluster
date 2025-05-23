from modules.logger import Logger

logger = Logger()

class LoadEvaluator:
    def __init__(self, cluster_state):
        self.cluster_state = cluster_state

    def evaluate_imbalance(self, metrics=None, aggressiveness=3):
        """
        Evaluate imbalance in resource distribution based on selected metrics and aggressiveness level.
        metrics: list of metrics to consider (cpu, memory, disk, network)
        """
        if metrics is None:
            metrics = ["cpu", "memory", "disk", "network"]
        logger.info(f"Evaluating load imbalance with metrics: {metrics} and aggressiveness level {aggressiveness}...")
        # Dummy logic: check if there is more than a 20% difference in VM count between clusters
        cluster_counts = [len(vms) for vms in self.cluster_state.values()]
        max_vms = max(cluster_counts)
        min_vms = min(cluster_counts)
        if (max_vms - min_vms) > 0.2 * max_vms:
            logger.warning("Imbalance detected by VM count!")
            return True
        # TODO: Add logic for cpu, memory, disk, network imbalance using metrics
        logger.info("No significant imbalance detected.")
        return False


