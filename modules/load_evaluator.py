import logging

logger = logging.getLogger(__name__)

class LoadEvaluator:
    def __init__(self, hosts):
        self.hosts = hosts

    def get_resource_percentage_lists(self):
        cpu_percentages = []
        mem_percentages = []
        disk_percentages = []
        net_percentages = []

        if not isinstance(self.hosts, list) or not self.hosts:
            logger.warning(f"[LoadEvaluator] Hosts list is not a list or is empty (type: {type(self.hosts)}). Cannot calculate percentage lists.")
            return [], [], [], []

        for host_data in self.hosts:
            if not isinstance(host_data, dict):
                logger.warning(f"[LoadEvaluator] Expected a dict for host_data, got {type(host_data)}. Skipping this host.")
                cpu_percentages.append(0.0)
                mem_percentages.append(0.0)
                disk_percentages.append(0.0)
                net_percentages.append(0.0)
                continue

            cpu_usage = host_data.get('cpu_usage', 0.0)
            cpu_capacity = host_data.get('cpu_capacity', 0.0)
            cpu_perc = (cpu_usage / cpu_capacity * 100.0) if cpu_capacity > 0 else 0.0
            cpu_percentages.append(cpu_perc)

            mem_usage = host_data.get('memory_usage', 0.0)
            mem_capacity = host_data.get('memory_capacity', 0.0)
            mem_perc = (mem_usage / mem_capacity * 100.0) if mem_capacity > 0 else 0.0
            mem_percentages.append(mem_perc)

            disk_usage = host_data.get('disk_io_usage', 0.0) 
            disk_capacity = host_data.get('disk_io_capacity', 0.0) 
            disk_perc = (disk_usage / disk_capacity * 100.0) if disk_capacity > 0 else 0.0
            disk_percentages.append(disk_perc)

            net_usage = host_data.get('network_io_usage', 0.0)
            net_capacity = host_data.get('network_capacity', 0.0)
            net_perc = (net_usage / net_capacity * 100.0) if net_capacity > 0 else 0.0
            net_percentages.append(net_perc)
            
        return cpu_percentages, mem_percentages, disk_percentages, net_percentages

    def get_thresholds(self, aggressiveness=3):
        mapping = {
            5: 5.0,
            4: 10.0,
            3: 15.0,
            2: 20.0,
            1: 25.0
        }
        threshold_value = mapping.get(aggressiveness, 15.0) 
        
        if aggressiveness not in mapping:
            logger.warning(f"[LoadEvaluator] Invalid aggressiveness level: {aggressiveness}. Defaulting to threshold: {threshold_value}%.")

        thresholds = {
            'cpu': threshold_value,
            'memory': threshold_value,
            'disk': threshold_value,
            'network': threshold_value
        }
        logger.debug(f"[LoadEvaluator] Aggressiveness: {aggressiveness}, Max Difference Thresholds: {thresholds}")
        return thresholds

    def evaluate_imbalance(self, metrics_to_check=None, aggressiveness=3):
        cpu_percentages, mem_percentages, disk_percentages, net_percentages = self.get_resource_percentage_lists()
        
        all_metrics_data = {
            'cpu': cpu_percentages,
            'memory': mem_percentages,
            'disk': disk_percentages,
            'network': net_percentages
        }

        if metrics_to_check is None:
            metrics_to_check = ['cpu', 'memory', 'disk', 'network']

        allowed_thresholds = self.get_thresholds(aggressiveness) 
        imbalance_found = False

        for resource_name in metrics_to_check:
            percentages = all_metrics_data.get(resource_name)
            
            if not percentages or len(percentages) < 2:
                logger.debug(f"[LoadEvaluator] Not enough data points for resource '{resource_name}' (count: {len(percentages) if percentages else 0}). Considered balanced.")
                continue

            current_min_usage = min(percentages)
            current_max_usage = max(percentages)
            current_diff = current_max_usage - current_min_usage
            
            resource_threshold = allowed_thresholds.get(resource_name)
            if resource_threshold is None: 
                logger.error(f"[LoadEvaluator] Critical: No threshold defined for resource: {resource_name}.")
                continue 

            logger.info(f"[LoadEvaluator] Resource '{resource_name}': Min Usage={current_min_usage:.2f}%, Max Usage={current_max_usage:.2f}%, Difference={current_diff:.2f}%")

            if current_diff > resource_threshold:
                logger.warning(f"[LoadEvaluator] Resource '{resource_name}' is imbalanced. Difference {current_diff:.2f}% > Threshold {resource_threshold:.2f}% (Aggressiveness: {aggressiveness})")
                imbalance_found = True
            else:
                logger.info(f"[LoadEvaluator] Resource '{resource_name}' is balanced. Difference {current_diff:.2f}% <= Threshold {resource_threshold:.2f}% (Aggressiveness: {aggressiveness})")

        return imbalance_found

    def is_balanced(self, metrics=None, aggressiveness=3):
        return not self.evaluate_imbalance(metrics_to_check=metrics, aggressiveness=aggressiveness)

    def get_resource_usage_lists(self):
        if not isinstance(self.hosts, list) or not all(isinstance(h, dict) for h in self.hosts if h is not None): # Added check for h is not None
            logger.error("[LoadEvaluator] self.hosts is not a list of dictionaries or contains None values.")
            return [], [], [], []

        cpu_usage = [metrics.get('cpu_usage', 0.0) for metrics in self.hosts if metrics]
        mem_usage = [metrics.get('memory_usage', 0.0) for metrics in self.hosts if metrics]
        disk_io = [metrics.get('disk_io_usage', 0.0) for metrics in self.hosts if metrics]
        net_io = [metrics.get('network_io_usage', 0.0) for metrics in self.hosts if metrics]
        
        self.cluster_totals = {
            'cpu': sum(cpu_usage),
            'memory': sum(mem_usage),
            'disk_io': sum(disk_io),
            'network_io': sum(net_io)
        }
        
        num_hosts = len([h for h in self.hosts if h]) # Count non-None hosts
        self.target_per_host = {
            'cpu': self.cluster_totals['cpu'] / num_hosts if num_hosts > 0 else 0,
            'memory': self.cluster_totals['memory'] / num_hosts if num_hosts > 0 else 0,
            'disk_io': self.cluster_totals['disk_io'] / num_hosts if num_hosts > 0 else 0,
            'network_io': self.cluster_totals['network_io'] / num_hosts if num_hosts > 0 else 0
        }
        
        self.resource_deviations = {
            'cpu': [abs(usage - self.target_per_host['cpu']) for usage in cpu_usage],
            'memory': [abs(usage - self.target_per_host['memory']) for usage in mem_usage],
            'disk_io': [abs(usage - self.target_per_host['disk_io']) for usage in disk_io],
            'network_io': [abs(usage - self.target_per_host['network_io']) for usage in net_io]
        }
        return cpu_usage, mem_usage, disk_io, net_io
