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
        """
        Fetches the performance data for a specific entity (VM or Host).
        :param entity: The VM or Host object
        :param metric_name: The metric name to be fetched (CPU, Memory, etc.)
        :param interval: The sampling interval in seconds (default 20s)
        :return: A list of metric data
        """
        try:
            metric_id = self.counter_map.get(metric_name)
            if not metric_id:
                logger.warning(f"Metric {metric_name} not found in counter map!")
                return None

            metrics = self.performance_manager.QueryPerf(
                querySpec=[
                    vim.PerformanceManager.QuerySpec(
                        entity=entity,
                        metricId=[vim.PerformanceManager.MetricId(counterId=metric_id, instance='')],
                        intervalId=interval,
                        maxSample=1  # Only take the latest sample
                    )
                ]
            )

            if metrics and metrics[0].value:
                return metrics[0].value
            return None

        except Exception as e:
            logger.error(f"Error fetching performance data for {metric_name}: {e}")
            return None

    def get_vm_metrics(self, vm):
        """
        Fetches the resource metrics (CPU, Memory, Disk I/O, Network I/O) for a given VM.
        :param vm: The virtual machine object
        :return: A dictionary with metrics (CPU, Memory, Disk IO, Network IO)
        """
        vm_metrics = {}

        # Define metrics to fetch
        metrics = {
            "CPU Usage (MHz)": "cpu.usage",
            "Memory Usage (MB)": "mem.usage",
            "Disk IO (MB/s)": "disk.usage",
            "Network IO (MB/s)": "net.usage"
        }

        for metric_name, metric_key in metrics.items():
            metric_data = self._get_performance_data(vm, metric_key)
            if metric_data and len(metric_data) > 0:
                # Convert units if necessary
                if metric_name == "Memory Usage (MB)":
                    vm_metrics[metric_name] = metric_data[0].value / 1024.0  # Assuming data is in KB
                else:
                    vm_metrics[metric_name] = metric_data[0].value
            else:
                vm_metrics[metric_name] = None  # Handle missing data gracefully

        return vm_metrics

    def get_host_metrics(self, host):
        """
        Fetches the resource metrics (CPU, Memory, Disk I/O, Network I/O) for a given Host.
        :param host: The ESXi host object
        :return: A dictionary with metrics (CPU, Memory, Disk IO, Network IO)
        """
        host_metrics = {}

        # Define metrics to fetch
        metrics = {
            "CPU Usage (MHz)": "cpu.usage",
            "Memory Usage (MB)": "mem.usage",
            "Disk IO (MB/s)": "disk.usage",
            "Network IO (MB/s)": "net.usage"
        }

        for metric_name, metric_key in metrics.items():
            metric_data = self._get_performance_data(host, metric_key)
            if metric_data and len(metric_data) > 0:
                # Convert units if necessary
                if metric_name == "Memory Usage (MB)":
                    host_metrics[metric_name] = metric_data[0].value / 1024.0  # Assuming data is in KB
                else:
                    host_metrics[metric_name] = metric_data[0].value
            else:
                host_metrics[metric_name] = None  # Handle missing data gracefully

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

