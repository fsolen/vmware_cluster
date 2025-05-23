from pyVmomi import vim
from modules.logger import Logger

logger = Logger()

class ClusterState:
    def __init__(self, service_instance):
        self.service_instance = service_instance
        self.vms = self._get_all_vms()
        self.hosts = self._get_all_hosts()

    def _get_all_vms(self):
        """
        Return a flat list of all VM objects in all clusters in all datacenters.
        """
        vms = []
        content = self.service_instance.RetrieveContent()
        for datacenter in content.rootFolder.childEntity:
            if hasattr(datacenter, 'vmFolder'):
                vm_view = content.viewManager.CreateContainerView(datacenter.vmFolder, [vim.VirtualMachine], True)
                vms.extend(vm_view.view)
                vm_view.Destroy()
        return vms

    def _get_all_hosts(self):
        """
        Return a flat list of all HostSystem objects in all clusters in all datacenters.
        """
        hosts = []
        content = self.service_instance.RetrieveContent()
        for datacenter in content.rootFolder.childEntity:
            if hasattr(datacenter, 'hostFolder'):
                host_view = content.viewManager.CreateContainerView(datacenter.hostFolder, [vim.HostSystem], True)
                hosts.extend(host_view.view)
                host_view.Destroy()
        return hosts

    def get_cluster_state(self):
        """
        Retrieve and display the state of clusters and their associated VMs
        """
        logger.info("Fetching live cluster state...")

        if not self.service_instance:
            logger.error("No service_instance provided.")
            raise Exception("service_instance is None")

        content = self.service_instance.RetrieveContent()
        cluster_state = {}

        for datacenter in content.rootFolder.childEntity:
            if isinstance(datacenter, vim.Datacenter):
                for cluster in datacenter.hostFolder.childEntity:
                    if isinstance(cluster, vim.ClusterComputeResource):
                        vms = [vm.name for vm in cluster.resourcePool.vm]
                        cluster_state[cluster.name] = vms

        logger.info("Cluster state fetched successfully.")
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

    def annotate_hosts_with_metrics(self, resource_monitor):
        """
        Build a dictionary mapping host names to their metrics.
        """
        self.host_metrics = {}
        for host in self.hosts:
            metrics = resource_monitor.get_host_metrics(host)
            self.host_metrics[host.name] = {
                'cpu_usage': metrics.get("CPU Usage (MHz)", 0) or 0,
                'memory_usage': metrics.get("Memory Usage (MB)", 0) or 0,
                'disk_io_usage': metrics.get("Disk IO (MB/s)", 0) or 0,
                'network_io_usage': metrics.get("Network IO (MB/s)", 0) or 0,
                'host_obj': host
            }

    def get_vm_by_name(self, name):
        """
        Return the VM object with the given name, or None if not found.
        """
        for vm in self.vms:
            if vm.name == name:
                return vm
        return None
