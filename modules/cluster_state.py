from pyVmomi import vim
from modules.logger import Logger

logger = Logger()

class ClusterState:
    def __init__(self, service_instance):
        self.service_instance = service_instance
        self.vms = self._get_all_vms()
        self.hosts = self._get_all_hosts()

    def _get_all_vms(self):
        """Get all VMs in the datacenter."""
        content = self.service_instance.RetrieveContent()
        container = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.VirtualMachine], True
        )
        vms = container.view
        container.Destroy()
        
        # Filter out templates and powered off VMs
        return [vm for vm in vms if not vm.config.template and vm.runtime.powerState == 'poweredOn']

    def _get_all_hosts(self):
        """Get all ESXi hosts in the datacenter."""
        content = self.service_instance.RetrieveContent()
        container = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.HostSystem], True
        )
        hosts = container.view
        container.Destroy()
        
        # Filter out hosts that are not in connected state
        return [host for host in hosts if host.runtime.connectionState == 'connected']

    def get_cluster_state(self):
        """
        Get the current state of the cluster including all VMs and hosts with their metrics.
        Returns a dictionary with cluster state information.
        """
        if not hasattr(self, 'vms') or not hasattr(self, 'hosts'):
            self.vms = self._get_all_vms()
            self.hosts = self._get_all_hosts()
            
        cluster_state = {
            'vms': [],
            'hosts': [],
            'total_metrics': {
                'cpu_usage': 0,
                'memory_usage': 0,
                'disk_io_usage': 0,
                'network_io_usage': 0
            }
        }

        # Aggregate VM metrics
        for vm in self.vms:
            vm_metrics = self.vm_metrics.get(vm.name, {})
            vm_info = {
                'name': vm.name,
                'host': self.get_host_of_vm(vm),
                'cpu_usage': vm_metrics.get('cpu_usage', 0),
                'memory_usage': vm_metrics.get('memory_usage', 0),
                'disk_io_usage': vm_metrics.get('disk_io_usage', 0),
                'network_io_usage': vm_metrics.get('network_io_usage', 0)
            }
            cluster_state['vms'].append(vm_info)
            
            # Add to totals
            for metric in ['cpu_usage', 'memory_usage', 'disk_io_usage', 'network_io_usage']:
                cluster_state['total_metrics'][metric] += vm_info[metric]

        # Aggregate host metrics
        for host in self.hosts:
            host_metrics = self.host_metrics.get(host.name, {})
            host_info = {
                'name': host.name,
                'cpu_usage': host_metrics.get('cpu_usage', 0),
                'memory_usage': host_metrics.get('memory_usage', 0),
                'disk_io_usage': host_metrics.get('disk_io_usage', 0),
                'network_io_usage': host_metrics.get('network_io_usage', 0),
                'cpu_capacity': host_metrics.get('cpu_capacity', 0),
                'memory_capacity': host_metrics.get('memory_capacity', 0),
                'disk_io_capacity': host_metrics.get('disk_io_capacity', 0),
                'network_capacity': host_metrics.get('network_capacity', 0)
            }
            cluster_state['hosts'].append(host_info)

        return cluster_state
        
    def get_host_of_vm(self, vm):
        """
        Given a VM object, return the name of the host it is running on.
        """
        try:
            if hasattr(vm, 'runtime') and hasattr(vm.runtime, 'host') and vm.runtime.host:
                return vm.runtime.host.name
            else:
                logger.warning(f"VM '{vm.name}' does not have a valid host reference.")
                return None
        except Exception as e:
            logger.error(f"Error getting host for VM '{getattr(vm, 'name', str(vm))}': {e}")
            return None

    def annotate_vms_with_metrics(self, resource_monitor):
        """
        Build a dictionary mapping VM names to their absolute resource consumption metrics.
        These metrics represent actual resource usage that will be used to calculate host loads.
        """
        self.vm_metrics = {}
        for vm in self.vms:
            metrics = resource_monitor.get_vm_metrics(vm)
            
            # CPU usage in MHz
            cpu_usage = vm.summary.quickStats.overallCpuUsage or 0
            
            # Memory usage in MB
            memory_usage = vm.summary.quickStats.guestMemoryUsage or 0
            
            # Network IO in MBps (sum of all NICs transmitted + received)
            network_usage = 0
            for nic in vm.config.hardware.device:
                if isinstance(nic, vim.vm.device.VirtualEthernetCard):
                    net_tx = metrics.get(f"net.transmitted[{nic.deviceInfo.label}]", 0) or 0
                    net_rx = metrics.get(f"net.received[{nic.deviceInfo.label}]", 0) or 0
                    network_usage += (net_tx + net_rx)
            
            # Disk IO in MBps (sum of all disks read + write)
            disk_usage = 0
            for disk in vm.config.hardware.device:
                if isinstance(disk, vim.vm.device.VirtualDisk):
                    disk_read = metrics.get(f"disk.read[{disk.deviceInfo.label}]", 0) or 0
                    disk_write = metrics.get(f"disk.write[{disk.deviceInfo.label}]", 0) or 0
                    disk_usage += (disk_read + disk_write)
                    disk_metrics = metrics.get(f"disk.average[{disk.deviceInfo.label}]", 0) or 0
                    disk_usage += disk_metrics
            
            self.vm_metrics[vm.name] = {
                'cpu_usage': cpu_usage,          # MHz
                'memory_usage': memory_usage,    # MB
                'disk_io_usage': disk_usage,     # MBps
                'network_io_usage': network_usage, # MBps
                'vm_obj': vm
            }

    def annotate_hosts_with_metrics(self, resource_monitor):
        """
        Calculate host metrics by summing the resource consumption of VMs running on each host.
        This gives us the total load on each host based on VM resource usage.
        """
        self.host_metrics = {}
        for host in self.hosts:
            # Get host capacities
            cpu_capacity = host.hardware.cpuInfo.numCpuCores * host.hardware.cpuInfo.hz / (1000 * 1000)  # MHz
            memory_capacity = host.hardware.memorySize / (1024 * 1024)  # MB
            
            # Initialize host metrics
            host_metrics = {
                'cpu_usage': 0,
                'memory_usage': 0,
                'disk_io_usage': 0,
                'network_io_usage': 0,
                'cpu_capacity': cpu_capacity,
                'memory_capacity': memory_capacity,
                'vms': [],
                'host_obj': host
            }
            
            # Sum up resource usage from all VMs on this host
            for vm in self.get_vms_on_host(host):
                vm_metrics = self.vm_metrics.get(vm.name, {})
                host_metrics['cpu_usage'] += vm_metrics.get('cpu_usage', 0)
                host_metrics['memory_usage'] += vm_metrics.get('memory_usage', 0)
                host_metrics['disk_io_usage'] += vm_metrics.get('disk_io_usage', 0)
                host_metrics['network_io_usage'] += vm_metrics.get('network_io_usage', 0)
                host_metrics['vms'].append(vm.name)
            
            # Calculate usage percentages
            host_metrics['cpu_usage_pct'] = (host_metrics['cpu_usage'] / cpu_capacity * 100) if cpu_capacity > 0 else 0
            host_metrics['memory_usage_pct'] = (host_metrics['memory_usage'] / memory_capacity * 100) if memory_capacity > 0 else 0
            
            self.host_metrics[host.name] = host_metrics
            
            logger.info(f"Host {host.name} metrics:")
            logger.info(f"CPU: {host_metrics['cpu_usage_pct']:.1f}% ({host_metrics['cpu_usage']}/{cpu_capacity} MHz)")
            logger.info(f"Memory: {host_metrics['memory_usage_pct']:.1f}% ({host_metrics['memory_usage']}/{memory_capacity} MB)")
            logger.info(f"Disk I/O: {host_metrics['disk_io_usage']} MBps")
            logger.info(f"Network I/O: {host_metrics['network_io_usage']} MBps")
            logger.info(f"VMs: {', '.join(host_metrics['vms'])}\n")

    def get_vms_on_host(self, host):
        """
        Return list of VMs currently running on the specified host.
        """
        return [vm for vm in self.vms if self.get_host_of_vm(vm) == host.name]
        
    def get_vm_by_name(self, name):
        """
        Return the VM object with the given name, or None if not found.
        """
        for vm in self.vms:
            if vm.name == name:
                return vm
        return None

    def log_cluster_stats(self):
        """Log detailed cluster statistics including resource distribution"""
        if not hasattr(self, 'host_metrics') or not hasattr(self, 'vm_metrics'):
            logger.warning("Metrics not yet collected. Run update_metrics() first.")
            return

        total_cpu_capacity = 0
        total_mem_capacity = 0
        total_cpu_usage = 0
        total_mem_usage = 0
        total_disk_io = 0
        total_net_io = 0
        
        # Log overall cluster state
        logger.info("\n=== Cluster State Summary ===")
        
        # Host-level statistics
        logger.info("\n--- Host Resource Distribution ---")
        for host_name, metrics in self.host_metrics.items():
            total_cpu_capacity += metrics['cpu_capacity']
            total_mem_capacity += metrics['memory_capacity']
            total_cpu_usage += metrics['cpu_usage']
            total_mem_usage += metrics['memory_usage']
            total_disk_io += metrics['disk_io_usage']
            total_net_io += metrics['network_io_usage']
            
            logger.info(f"\nHost: {host_name}")
            logger.info(f"├─ CPU: {metrics['cpu_usage_pct']:.1f}% ({metrics['cpu_usage']}/{metrics['cpu_capacity']} MHz)")
            logger.info(f"├─ Memory: {metrics['memory_usage_pct']:.1f}% ({metrics['memory_usage']}/{metrics['memory_capacity']} MB)")
            logger.info(f"├─ Disk I/O: {metrics['disk_io_usage']:.1f} MBps")
            logger.info(f"├─ Network I/O: {metrics['network_io_usage']:.1f} MBps")
            logger.info(f"└─ VMs: {len(metrics['vms'])} ({', '.join(metrics['vms'])})")

        # VM distribution analysis
        logger.info("\n--- VM Resource Consumption ---")
        for vm_name, metrics in self.vm_metrics.items():
            host_name = self.get_host_of_vm(metrics['vm_obj'])
            logger.info(f"\nVM: {vm_name} (on {host_name})")
            logger.info(f"├─ CPU: {metrics['cpu_usage']} MHz")
            logger.info(f"├─ Memory: {metrics['memory_usage']} MB")
            logger.info(f"├─ Disk I/O: {metrics['disk_io_usage']:.1f} MBps")
            logger.info(f"└─ Network I/O: {metrics['network_io_usage']:.1f} MBps")

        # Overall cluster metrics
        cluster_cpu_usage = (total_cpu_usage / total_cpu_capacity * 100) if total_cpu_capacity > 0 else 0
        cluster_mem_usage = (total_mem_usage / total_mem_capacity * 100) if total_mem_capacity > 0 else 0
        
        logger.info("\n=== Cluster Total Resource Usage ===")
        logger.info(f"CPU: {cluster_cpu_usage:.1f}% ({total_cpu_usage}/{total_cpu_capacity} MHz)")
        logger.info(f"Memory: {cluster_mem_usage:.1f}% ({total_mem_usage}/{total_mem_capacity} MB)")
        logger.info(f"Total Disk I/O: {total_disk_io:.1f} MBps")
        logger.info(f"Total Network I/O: {total_net_io:.1f} MBps")
        logger.info(f"Total Hosts: {len(self.hosts)}")
        logger.info(f"Total VMs: {len(self.vms)}\n")

    def update_metrics(self, resource_monitor=None):
        """Update VM and Host metrics"""
        if resource_monitor is None:
            from .resource_monitor import ResourceMonitor
            resource_monitor = ResourceMonitor(self.cluster)

        logger.info("Updating cluster metrics...")
        self.annotate_vms_with_metrics(resource_monitor)
        self.annotate_hosts_with_metrics(resource_monitor)
        self.log_cluster_stats()  # Log detailed cluster state after updating metrics
