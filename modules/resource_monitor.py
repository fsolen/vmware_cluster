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

    def _get_performance_data(self, entity, metric_id, interval=20):
        """
        Fetches the performance data for a specific entity (VM or Host).
        :param entity: The VM or Host object
        :param metric_id: The metric ID to be fetched (CPU, Memory, etc.)
        :param interval: The sampling interval in seconds (default 20s)
        :return: A list of metric data
        """
        try:
            counter_info = self.performance_manager.queryAvailablePerfMetric(entity=entity)
            metric_id_list = [metric.counterId for metric in counter_info if metric.counterId == metric_id]
            if not metric_id_list:
                logger.warning(f"Metric ID {metric_id} not found!")
                return None

            metrics = self.performance_manager.queryPerf(
                entity=entity,
                metricId=metric_id_list,
                intervalId=interval,
                maxSample=1  # Only take the latest sample
            )

            if metrics:
                return metrics[0].value
            return None

        except Exception as e:
            logger.error(f"Error fetching performance data: {e}")
            return None

    def get_vm_metrics(self, vm):
        """
        Fetches the resource metrics (CPU, Memory, Disk I/O, Network I/O) for a given VM.
        :param vm: The virtual machine object
        :return: A dictionary with metrics (CPU, Memory, Disk IO, Network IO)
        """
        vm_metrics = {}

        # CPU
        cpu_metric_id = 6  # CPU usage metric ID
        cpu_usage = self._get_performance_data(vm, cpu_metric_id)
        if cpu_usage:
            vm_metrics['CPU Usage (MHz)'] = cpu_usage[0]

        # Memory
        memory_metric_id = 24  # Memory usage metric ID
        memory_usage = self._get_performance_data(vm, memory_metric_id)
        if memory_usage:
            vm_metrics['Memory Usage (MB)'] = memory_usage[0]

        # Disk I/O
        disk_metric_id = 15  # Disk Read/Write I/O metric ID
        disk_io = self._get_performance_data(vm, disk_metric_id)
        if disk_io:
            vm_metrics['Disk IO (MB/s)'] = disk_io[0]

        # Network I/O
        net_metric_id = 16  # Network usage metric ID
        net_io = self._get_performance_data(vm, net_metric_id)
        if net_io:
            vm_metrics['Network IO (MB/s)'] = net_io[0]

        return vm_metrics

    def get_host_metrics(self, host):
        """
        Fetches the resource metrics (CPU, Memory, Disk I/O, Network I/O) for a given Host.
        :param host: The ESXi host object
        :return: A dictionary with metrics (CPU, Memory, Disk IO, Network IO)
        """
        host_metrics = {}

        # CPU
        cpu_metric_id = 6  # CPU usage metric ID
        cpu_usage = self._get_performance_data(host, cpu_metric_id)
        if cpu_usage:
            host_metrics['CPU Usage (MHz)'] = cpu_usage[0]

        # Memory
        memory_metric_id = 24  # Memory usage metric ID
        memory_usage = self._get_performance_data(host, memory_metric_id)
        if memory_usage:
            host_metrics['Memory Usage (MB)'] = memory_usage[0]

        # Disk I/O
        disk_metric_id = 15  # Disk Read/Write I/O metric ID
        disk_io = self._get_performance_data(host, disk_metric_id)
        if disk_io:
            host_metrics['Disk IO (MB/s)'] = disk_io[0]

        # Network I/O
        net_metric_id = 16  # Network usage metric ID
        net_io = self._get_performance_data(host, net_metric_id)
        if net_io:
            host_metrics['Network IO (MB/s)'] = net_io[0]

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

