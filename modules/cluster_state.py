from pyVmomi import vim
from modules.logger import Logger

logger = Logger()

class ClusterState:
    def __init__(self, service_instance):
        self.service_instance = service_instance
        self.vms = self._get_all_vms()

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

    def get_vm_by_name(self, vm_name):
        """
        Given a VM name, return the VM object from self.vms.
        """
        for vm in self.vms:
            if vm.name == vm_name:
                return vm
        logger.warning(f"VM with name '{vm_name}' not found in cluster state.")
        return None
