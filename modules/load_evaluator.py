import statistics
import logging

logger = logging.getLogger(__name__)

class LoadEvaluator:
    def __init__(self, hosts):
        self.hosts = hosts  # List of Host objects with resource attributes

    def get_resource_lists(self):
        cpu = [host.cpu_usage for host in self.hosts]
        mem = [host.memory_usage for host in self.hosts]
        disk = [host.disk_usage for host in self.hosts]
        net = [host.network_usage for host in self.hosts]
        return cpu, mem, disk, net

    def get_thresholds(self, aggressiveness=3):
        """
        Returns thresholds for each resource based on aggressiveness (1-5).
        Higher aggressiveness means lower thresholds (more sensitive).
        """
        # Base thresholds (in % stdev) for lowest aggressiveness
        base_thresholds = {
            'cpu': 20.0,
            'memory': 20.0,
            'disk': 20.0,
            'network': 20.0
        }
        # Each step of aggressiveness reduces threshold by 2.5% (tune as needed)
        step = 2.5
        factor = (aggressiveness - 1) * step
        thresholds = {k: max(5.0, v - factor) for k, v in base_thresholds.items()}
        return thresholds

    def get_resource_usage_lists(self):
        """
        Get absolute resource usage values and calculate cluster totals and averages
        """
        # Get absolute usage values
        cpu_usage = [metrics['cpu_usage'] for metrics in self.hosts]
        mem_usage = [metrics['memory_usage'] for metrics in self.hosts]
        disk_io = [metrics['disk_io_usage'] for metrics in self.hosts]
        net_io = [metrics['network_io_usage'] for metrics in self.hosts]
        
        # Calculate cluster totals
        self.cluster_totals = {
            'cpu': sum(cpu_usage),
            'memory': sum(mem_usage),
            'disk_io': sum(disk_io),
            'network_io': sum(net_io)
        }
        
        # Calculate ideal average per host
        num_hosts = len(self.hosts)
        self.target_per_host = {
            'cpu': self.cluster_totals['cpu'] / num_hosts if num_hosts > 0 else 0,
            'memory': self.cluster_totals['memory'] / num_hosts if num_hosts > 0 else 0,
            'disk_io': self.cluster_totals['disk_io'] / num_hosts if num_hosts > 0 else 0,
            'network_io': self.cluster_totals['network_io'] / num_hosts if num_hosts > 0 else 0
        }
        
        # Calculate deviations from target
        self.resource_deviations = {
            'cpu': [abs(usage - self.target_per_host['cpu']) for usage in cpu_usage],
            'memory': [abs(usage - self.target_per_host['memory']) for usage in mem_usage],
            'disk_io': [abs(usage - self.target_per_host['disk_io']) for usage in disk_io],
            'network_io': [abs(usage - self.target_per_host['network_io']) for usage in net_io]
        }
        
        logger.info("Cluster resource distribution:")
        logger.info(f"Total CPU: {self.cluster_totals['cpu']} MHz (Target per host: {self.target_per_host['cpu']:.0f} MHz)")
        logger.info(f"Total Memory: {self.cluster_totals['memory']} MB (Target per host: {self.target_per_host['memory']:.0f} MB)")
        logger.info(f"Total Disk I/O: {self.cluster_totals['disk_io']} MBps (Target per host: {self.target_per_host['disk_io']:.0f} MBps)")
        logger.info(f"Total Network I/O: {self.cluster_totals['network_io']} MBps (Target per host: {self.target_per_host['network_io']:.0f} MBps)")
        
        return cpu_usage, mem_usage, disk_io, net_io

    def evaluate_imbalance(self, metrics=None, aggressiveness=3):
        """
        Evaluate imbalance for selected metrics and aggressiveness.
        metrics: list of resource names to check (cpu, memory, disk, network)
        """
        cpu, mem, disk, net = self.get_resource_percentage_lists()
        all_metrics = {'cpu': cpu, 'memory': mem, 'disk': disk, 'network': net}
        if metrics is None:
            metrics = ['cpu', 'memory', 'disk', 'network']
        imbalance = {
            k: statistics.stdev(all_metrics[k]) if len(all_metrics[k]) > 1 else 0
            for k in metrics
        }
        logger.info(f"Resource imbalance (percentage stdev): {imbalance}")
        thresholds = self.get_thresholds(aggressiveness)
        for resource, value in imbalance.items():
            if value > thresholds.get(resource, float('inf')):
                logger.info(f"Resource '{resource}' is imbalanced: {value:.2f}% > {thresholds[resource]}%")
                return True  # Imbalance detected
        return False  # No imbalance

    def is_balanced(self, metrics=None, aggressiveness=3):
        """
        Returns True if all resources are balanced within their respective thresholds (in % stdev).
        """
        return not self.evaluate_imbalance(metrics=metrics, aggressiveness=aggressiveness)