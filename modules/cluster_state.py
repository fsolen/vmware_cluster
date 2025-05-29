from pyVmomi import vim
import logging

logger = logging.getLogger('fdrs')

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
        for vm_obj in self.vms: # Renamed vm to vm_obj for clarity
            vm_metrics_data = self.vm_metrics.get(vm_obj.name, {}) # Renamed vm_metrics to vm_metrics_data
            vm_info = {
                'name': vm_obj.name,
                'host': self.get_host_of_vm(vm_obj), # Pass vm_obj
                'cpu_usage': vm_metrics_data.get('cpu_usage_abs', 0), # Use _abs for consistency
                'memory_usage': vm_metrics_data.get('memory_usage_abs', 0), # Use _abs
                'disk_io_usage': vm_metrics_data.get('disk_io_usage_abs', 0), # Use _abs
                'network_io_usage': vm_metrics_data.get('network_io_usage_abs', 0) # Use _abs
            }
            cluster_state['vms'].append(vm_info)
            
            # Add to totals using the same keys
            for metric_key in ['cpu_usage', 'memory_usage', 'disk_io_usage', 'network_io_usage']:
                cluster_state['total_metrics'][metric_key] += vm_info[metric_key]

        # Aggregate host metrics
        for host_obj in self.hosts: # Renamed host to host_obj
            host_metrics_data = self.host_metrics.get(host_obj.name, {}) # Renamed host_metrics
            host_info = {
                'name': host_obj.name,
                'cpu_usage': host_metrics_data.get('cpu_usage', 0), # From annotate_hosts_with_metrics
                'memory_usage': host_metrics_data.get('memory_usage', 0), # From annotate_hosts_with_metrics
                'disk_io_usage': host_metrics_data.get('disk_io_usage', 0), # From annotate_hosts_with_metrics
                'network_io_usage': host_metrics_data.get('network_io_usage', 0), # From annotate_hosts_with_metrics
                'cpu_capacity': host_metrics_data.get('cpu_capacity', 0),
                'memory_capacity': host_metrics_data.get('memory_capacity', 0),
                'disk_io_capacity': host_metrics_data.get('disk_io_capacity', 0), # From ResourceMonitor via annotate_hosts
                'network_capacity': host_metrics_data.get('network_capacity', 0) # From ResourceMonitor via annotate_hosts
            }
            cluster_state['hosts'].append(host_info)

        return cluster_state
        
    def get_host_of_vm(self, vm_object): # Renamed vm to vm_object
        """
        Given a VM object, return the name of the host it is running on.
        """
        try:
            # Use vm_object consistently
            if hasattr(vm_object, 'runtime') and hasattr(vm_object.runtime, 'host') and vm_object.runtime.host:
                return vm_object.runtime.host # Return the host object itself
            else:
                logger.warning(f"VM '{vm_object.name}' does not have a valid host reference.")
                return None
        except Exception as e:
            logger.error(f"Error getting host for VM '{getattr(vm_object, 'name', str(vm_object))}': {e}")
            return None

    def annotate_vms_with_metrics(self, resource_monitor):
        """
        Build a dictionary mapping VM names to their absolute resource consumption metrics.
        These metrics represent actual resource usage that will be used to calculate host loads.
        Uses ResourceMonitor for I/O metrics and vm.summary.quickStats for absolute CPU/Memory.
        """
        self.vm_metrics = {}
        logger.info("[ClusterState] Starting annotation of VMs with metrics...") # Add overall start log
        for vm_obj in self.vms: # Renamed vm to vm_obj
            # --- START NEW LOG LINE ---
            vm_name_for_log = getattr(vm_obj, 'name', 'UnknownVMObject')
            logger.info(f"[ClusterState.annotate_vms] Processing VM: {vm_name_for_log}, Type: {type(vm_obj)}")
            if not hasattr(vm_obj, '_moId') or vm_obj._moId is None:
                 logger.warning(f"[ClusterState.annotate_vms] VM {vm_name_for_log} has missing or None _moId. Skipping its metric annotation.")
                 continue
            # --- END NEW LOG LINE ---

            # Get I/O metrics from ResourceMonitor (already in MBps)
            rm_vm_metrics = resource_monitor.get_vm_metrics(vm_obj)
            
            self.vm_metrics[vm_obj.name] = {
                # Absolute CPU usage in MHz from vSphere
                'cpu_usage_abs': vm_obj.summary.quickStats.overallCpuUsage or 0,
                # Absolute memory usage in MB from vSphere (guest memory usage)
                'memory_usage_abs': vm_obj.summary.quickStats.guestMemoryUsage or 0,
                # Disk I/O in MBps from ResourceMonitor
                'disk_io_usage_abs': rm_vm_metrics.get('disk_io_usage', 0.0),
                # Network I/O in MBps from ResourceMonitor
                'network_io_usage_abs': rm_vm_metrics.get('network_io_usage', 0.0),
                'vm_obj': vm_obj # Store the VM object itself
            }
        logger.info("[ClusterState] Finished annotation of VMs with metrics.") # Add overall end log

    def annotate_hosts_with_metrics(self, resource_monitor):
        """
        Calculate host metrics by summing the resource consumption of VMs running on each host.
        Also incorporates capacity information obtained directly or via ResourceMonitor for consistency.
        """
        self.host_metrics = {}
        logger.info("[ClusterState] Starting annotation of hosts with metrics...") # Add overall start log
        for host_obj in self.hosts: # Renamed host to host_obj
            # --- START NEW LOG LINE ---
            host_name_for_log = getattr(host_obj, 'name', 'UnknownHostObject')
            logger.info(f"[ClusterState.annotate_hosts] Processing host: {host_name_for_log}, Type: {type(host_obj)}")
            if not hasattr(host_obj, '_moId') or host_obj._moId is None:
                 logger.warning(f"[ClusterState.annotate_hosts] Host {host_name_for_log} has missing or None _moId. Skipping its metric annotation.")
                 continue
            # --- END NEW LOG LINE ---

            # Get host capacity info directly from host_obj or via resource_monitor if it normalizes/caches them
            # ResourceMonitor.get_host_metrics already includes capacities.
            # Let's ensure we use what ResourceMonitor provides for capacities for consistency,
            # especially for estimated ones like disk/network.
            
            rm_host_metrics = resource_monitor.get_host_metrics(host_obj)

            # Initialize host metrics structure
            current_host_metrics = {
                'cpu_usage': 0, # Sum of VM absolute CPU usage
                'memory_usage': 0, # Sum of VM absolute Memory usage
                'disk_io_usage': 0, # Sum of VM absolute Disk IO
                'network_io_usage': 0, # Sum of VM absolute Network IO
                'cpu_capacity': rm_host_metrics.get('cpu_capacity', 0),
                'memory_capacity': rm_host_metrics.get('memory_capacity', 0),
                'disk_io_capacity': rm_host_metrics.get('disk_io_capacity', 1), # Default to 1 to avoid div by zero
                'network_capacity': rm_host_metrics.get('network_capacity', 1), # Default to 1
                'vms': [],
                'host_obj': host_obj # Store the host object itself
            }
            
            # Sum up resource usage from all VMs on this host
            for vm_on_host in self.get_vms_on_host(host_obj): # Renamed vm to vm_on_host
                vm_metrics_data = self.vm_metrics.get(vm_on_host.name, {})
                current_host_metrics['cpu_usage'] += vm_metrics_data.get('cpu_usage_abs', 0)
                current_host_metrics['memory_usage'] += vm_metrics_data.get('memory_usage_abs', 0)
                current_host_metrics['disk_io_usage'] += vm_metrics_data.get('disk_io_usage_abs', 0)
                current_host_metrics['network_io_usage'] += vm_metrics_data.get('network_io_usage_abs', 0)
                current_host_metrics['vms'].append(vm_on_host.name)
            
            # Calculate usage percentages based on summed absolute VM consumptions and host capacities
            current_host_metrics['cpu_usage_pct'] = (current_host_metrics['cpu_usage'] / current_host_metrics['cpu_capacity'] * 100) \
                if current_host_metrics['cpu_capacity'] > 0 else 0
            current_host_metrics['memory_usage_pct'] = (current_host_metrics['memory_usage'] / current_host_metrics['memory_capacity'] * 100) \
                if current_host_metrics['memory_capacity'] > 0 else 0
            # For Disk and Network IO, LoadEvaluator will calculate percentages using these absolute values
            # and the capacities obtained from ResourceMonitor. So, no _pct needed here for disk/network.
            
            self.host_metrics[host_obj.name] = current_host_metrics
            
            # Logging can be verbose, ensure it's needed or adjust level/content
            logger.debug(f"Host {host_obj.name} annotated metrics:") # Changed to debug
            logger.debug(f"  CPU: {current_host_metrics['cpu_usage_pct']:.1f}% ({current_host_metrics['cpu_usage']}/{current_host_metrics['cpu_capacity']} MHz)")
            logger.debug(f"  Memory: {current_host_metrics['memory_usage_pct']:.1f}% ({current_host_metrics['memory_usage']}/{current_host_metrics['memory_capacity']} MB)")
            logger.debug(f"  Disk I/O: {current_host_metrics['disk_io_usage']:.1f} MBps (Capacity: {current_host_metrics['disk_io_capacity']:.1f} MBps)")
            logger.debug(f"  Network I/O: {current_host_metrics['network_io_usage']:.1f} MBps (Capacity: {current_host_metrics['network_capacity']:.1f} MBps)")
            logger.debug(f"  VMs: {', '.join(current_host_metrics['vms'])}\n")
        logger.info("[ClusterState] Finished annotation of hosts with metrics.") # Add overall end log

    def get_vms_on_host(self, host_object): # Renamed host to host_object
        """
        Return list of VMs currently running on the specified host.
        """
        return [vm_obj for vm_obj in self.vms if self.get_host_of_vm(vm_obj) == host_object.name] # Use host_object.name
        
    def get_vm_by_name(self, vm_name): # Renamed name to vm_name
        """
        Return the VM object with the given name, or None if not found.
        """
        for vm_obj in self.vms: # Renamed vm to vm_obj
            if vm_obj.name == vm_name:
                return vm_obj
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
            logger.info(f"├─ CPU: {metrics.get('cpu_usage_abs', 0)} MHz")
            logger.info(f"├─ Memory: {metrics.get('memory_usage_abs', 0)} MB")
            logger.info(f"├─ Disk I/O: {metrics.get('disk_io_usage_abs', 0):.1f} MBps")
            logger.info(f"└─ Network I/O: {metrics.get('network_io_usage_abs', 0):.1f} MBps")

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
            # This import and instantiation should be handled by the caller (fdrs.py)
            # However, to prevent breaking if called without one, we can keep it but log a warning.
            # For now, fixing the immediate bug of self.cluster vs self.service_instance
            from .resource_monitor import ResourceMonitor # Keep local import for safety
            logger.warning("ResourceMonitor not provided to update_metrics, creating a new instance. This is not recommended for production.")
            resource_monitor = ResourceMonitor(self.service_instance) # Corrected: self.service_instance

        logger.info("Updating cluster metrics...")
        self.annotate_vms_with_metrics(resource_monitor)
        self.annotate_hosts_with_metrics(resource_monitor)
        self.log_cluster_stats()  # Log detailed cluster state after updating metrics
