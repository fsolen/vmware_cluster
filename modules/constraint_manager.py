from collections import defaultdict
from modules.logger import Logger

logger = Logger()

class ConstraintManager:
    """
    Manages Anti-Affinity rules and distribution of VMs across clusters based on hostname prefixes
    """

    def __init__(self, service_instance):
        self.service_instance = service_instance
        self.vm_to_cluster_map = defaultdict(list)

    def get_vms(self):
        """
        Fetches all VMs in vCenter and their respective clusters.
        :return: A list of VMs with cluster information
        """
        vms = []
        clusters = self.service_instance.content.viewManager.CreateContainerView(
            self.service_instance.content.rootFolder, [vim.ClusterComputeResource], True
        )

        for cluster in clusters.view:
            for host in cluster.host:
                for vm in host.vm:
                    vms.append({"vm": vm, "cluster": cluster.name, "host": host.name})
        return vms

    def apply_affinity_rules(self, vms):
        """
        Apply the anti-affinity rule for VM distribution based on hostname prefixes.
        Distribute VMs across clusters evenly.
        :param vms: List of VMs to be processed
        :return: Updated mapping of VMs to clusters
        """
        # Group VMs by their hostname prefix (excluding the last 2 characters)
        vm_groups = defaultdict(list)
        for vm_info in vms:
            vm_name = vm_info["vm"].name
            prefix = vm_name[:-2]  # Exclude the last 2 characters from the hostname
            vm_groups[prefix].append(vm_info)

        # Distribute VMs evenly across available clusters
        available_clusters = list(set(vm_info["cluster"] for vm_info in vms))
        for prefix, group_vms in vm_groups.items():
            logger.info(f"Applying anti-affinity rule for prefix: {prefix}")
            
            cluster_idx = 0  # Start from the first cluster and distribute evenly
            for vm_info in group_vms:
                cluster = available_clusters[cluster_idx]
                self.vm_to_cluster_map[cluster].append(vm_info["vm"])
                logger.info(f"VM: {vm_info['vm'].name} assigned to cluster: {cluster}")
                cluster_idx = (cluster_idx + 1) % len(available_clusters)

        return self.vm_to_cluster_map

    def display_vm_distribution(self):
        """
        Displays the current distribution of VMs across clusters
        """
        logger.info("Current VM Distribution:")
        for cluster, vms in self.vm_to_cluster_map.items():
            logger.info(f"Cluster: {cluster}, VMs: {[vm.name for vm in vms]}")

    def apply(self):
        """
        Main method to run the affinity/anti-affinity rule and display results.
        """
        # Step 1: Get all VMs and their current cluster assignments
        vms = self.get_vms()

        # Step 2: Apply anti-affinity rule and distribute VMs across clusters
        self.apply_affinity_rules(vms)

        # Step 3: Display the final distribution of VMs
        self.display_vm_distribution()

