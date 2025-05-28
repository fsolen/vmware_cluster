from pyVmomi import vim
import time
from modules.logger import Logger

logger = Logger()

class ResourceMonitor:
    """
    Monitor resources (CPU, Memory, Disk I/O, Network I/O) of VMs and Hosts
    """

    def __init__(self, service_instance):
        self.service_instance = service_instance
        self.performance_manager = service_instance.content.perfManager
        self.counter_map = self._build_counter_map()

    def _build_counter_map(self):
        """
        Builds a map of performance counter names to IDs.
        """
        counter_map = {}
        perf_dict = {}
        perfList = self.performance_manager.perfCounter
        for counter in perfList:
            perf_dict[counter.groupInfo.key + "." + counter.nameInfo.key] = counter.key
        counter_map['cpu.usage'] = perf_dict.get('cpu.usage')
        counter_map['mem.usage'] = perf_dict.get('mem.usage')
        counter_map['disk.usage'] = perf_dict.get('disk.usage')
        counter_map['net.usage'] = perf_dict.get('net.usage')
        return counter_map

    def _get_performance_data(self, entity, metric_name, interval=20):
        content = self.service_instance.RetrieveContent()
        metric_id = self.counter_map.get(metric_name)
        if not metric_id:
            logger.warning(f"Metric ID for {metric_name} not found in counter map for entity {entity.name if hasattr(entity, 'name') else 'unknown entity'}!")
            return None

        try:
            query_results = self.performance_manager.QueryPerf(
                querySpec=[
                    vim.PerformanceManager.QuerySpec(
                        entity=entity,
                        metricId=[vim.PerformanceManager.MetricId(counterId=metric_id, instance='')],
                        intervalId=interval,
                        maxSample=1
                    )
                ]
            )

            if query_results and len(query_results) > 0:
                metric_series_list = query_results[0].value # This is a list of MetricSeries objects
                if metric_series_list and len(metric_series_list) > 0:
                    metric_series = metric_series_list[0] # First MetricSeries
                    if hasattr(metric_series, 'value') and metric_series.value and len(metric_series.value) > 0:
                        # metric_series.value is the list of actual data points (e.g., integers for IntSeries)
                        scalar_value = metric_series.value[0]
                        if scalar_value is None: # Handle if the specific sample value is None
                            logger.warning(f"Metric {metric_name} for {entity.name if hasattr(entity, 'name') else 'entity'} has a None value in its series.")
                            return 0 # Default to 0 if specific sample value is None
                        return scalar_value # Return the scalar value
                    else:
                        logger.warning(f"Metric {metric_name} for {entity.name if hasattr(entity, 'name') else 'entity'} has empty or missing 'value' list in its series.")
                else:
                    logger.warning(f"No metric series list found for {metric_name} on {entity.name if hasattr(entity, 'name') else 'entity'}.")
            else:
                logger.info(f"No performance data returned for {metric_name} on {entity.name if hasattr(entity, 'name') else 'entity'}. This might be normal if the counter is not applicable or data is not yet available.")
            return 0 # Default to 0 if no data found or any other issue
        except Exception as e:
            # Log specifics about the entity if possible
            entity_name = getattr(entity, 'name', 'unknown entity')
            logger.error(f"Error fetching scalar performance data for {metric_name} on {entity_name}: {e}")
            return 0 # Default to 0 on error

    def get_vm_metrics(self, vm):
        vm_metrics = {}
        metrics_to_fetch = { # Renamed 'metrics' to 'metrics_to_fetch' to avoid confusion
            "cpu_usage": "cpu.usage",       # Percentage 0-10000
            "memory_usage": "mem.usage",    # Percentage 0-10000
            "disk_io_usage": "disk.usage",  # Assuming KBps (e.g. from a counter like disk.read/write aggregated)
            "network_io_usage": "net.usage" # Assuming KBps (e.g. from a counter like net.tx/rx aggregated)
        }

        for metric_key, counter_key in metrics_to_fetch.items():
            scalar_metric_value = self._get_performance_data(vm, counter_key)

            if scalar_metric_value is None: # Should not happen if _get_performance_data defaults to 0
                scalar_metric_value = 0.0

            if metric_key == "cpu_usage":      # Counter value is 0-10000 (e.g., 5000 means 50%)
                vm_metrics[metric_key] = scalar_metric_value / 100.0
            elif metric_key == "memory_usage": # Counter value is 0-10000 (e.g., 5000 means 50%)
                vm_metrics[metric_key] = scalar_metric_value / 100.0
            elif metric_key == "disk_io_usage": # Assuming result from counter is in KBps
                vm_metrics[metric_key] = scalar_metric_value / 1024.0 # Convert to MBps
            elif metric_key == "network_io_usage": # Assuming result from counter is in KBps
                vm_metrics[metric_key] = scalar_metric_value / 1024.0 # Convert to MBps
            else:
                vm_metrics[metric_key] = scalar_metric_value # Should not be reached with current keys

        return vm_metrics

    def get_host_metrics(self, host):
        host_metrics = {}
        metrics_to_fetch = {
            "cpu_usage": "cpu.usage",       # Percentage 0-10000
            "memory_usage": "mem.usage",    # Percentage 0-10000
            "disk_io_usage": "disk.usage",  # Assuming KBps
            "network_io_usage": "net.usage" # Assuming KBps
        }

        for metric_key, counter_key in metrics_to_fetch.items():
            scalar_metric_value = self._get_performance_data(host, counter_key)

            if scalar_metric_value is None: # Should not happen if _get_performance_data defaults to 0
                scalar_metric_value = 0.0

            if metric_key == "cpu_usage":      # Counter value is 0-10000
                host_metrics[metric_key] = scalar_metric_value / 100.0
            elif metric_key == "memory_usage": # Counter value is 0-10000
                host_metrics[metric_key] = scalar_metric_value / 100.0
            elif metric_key == "disk_io_usage": # Assuming KBps
                host_metrics[metric_key] = scalar_metric_value / 1024.0 # Convert to MBps
            elif metric_key == "network_io_usage": # Assuming KBps
                host_metrics[metric_key] = scalar_metric_value / 1024.0 # Convert to MBps
            else:
                host_metrics[metric_key] = scalar_metric_value

        # Add capacity information
        host_metrics["cpu_capacity"] = host.summary.hardware.numCpuCores * host.summary.hardware.cpuMhz
        host_metrics["memory_capacity"] = host.summary.hardware.memorySize / (1024 * 1024)  # Convert B to MB
        
        # Set reasonable defaults for IO capacities (These are rough estimates, real values are hard to get)
        host_metrics["disk_io_capacity"] = 1000  # Example: 1000 MB/s 
        host_metrics["network_capacity"] = (host.hardware.networkInfo.pnic[0].linkSpeed * len(host.hardware.networkInfo.pnic)) / 8.0 if host.hardware.networkInfo and host.hardware.networkInfo.pnic else 1250 # Example: 10 Gbps in MB/s (10000 / 8)

        return host_metrics

    def monitor(self, interval=5):
        """
        Main loop to monitor all VMs and Hosts every 'interval' seconds.
        :param interval: The time between each metric fetch (default 5 seconds)
        """
        while True:
            # Fetch VM metrics
            logger.info("Fetching VM metrics...")
            vms = self.service_instance.content.viewManager.CreateContainerView(
                self.service_instance.content.rootFolder, [vim.VirtualMachine], True
            )
            for vm in vms.view:
                vm_metrics = self.get_vm_metrics(vm)
                if vm_metrics:
                    logger.info(f"VM: {vm.name} Metrics: {vm_metrics}")

            # Fetch Host metrics
            logger.info("Fetching Host metrics...")
            hosts = self.service_instance.content.viewManager.CreateContainerView(
                self.service_instance.content.rootFolder, [vim.HostSystem], True
            )
            for host in hosts.view:
                host_metrics = self.get_host_metrics(host)
                if host_metrics:
                    logger.info(f"Host: {host.name} Metrics: {host_metrics}")

            # Sleep for the interval
            time.sleep(interval)
