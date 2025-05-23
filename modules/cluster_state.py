from pyVmomi import vim
from modules.logger import Logger

logger = Logger()

class ClusterState:
    def __init__(self, service_instance):
        self.service_instance = service_instance

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
            for cluster in datacenter.hostFolder.childEntity:
                if isinstance(cluster, vim.ClusterComputeResource):
                    vms = [vm.name for vm in cluster.vm]
                    cluster_state[cluster.name] = vms

        logger.info("Cluster state fetched successfully.")
        return cluster_state
