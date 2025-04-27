from pyVmomi import vim
from modules.logger import Logger
from modules.connection_manager import ConnectionManager

logger = Logger()

class MigrationManager:
    """
    Manages the migration of VMs based on the affinity/anti-affinity distribution
    """

    def __init__(self, service_instance, vm_to_cluster_map, dry_run=False):
        self.service_instance = service_instance
        self.vm_to_cluster_map = vm_to_cluster_map
        self.dry_run = dry_run

    def _migrate_vm(self, vm, target_cluster):
        """
        Migrate a VM to a new cluster using vMotion
        :param vm: The VM object to migrate
        :param target_cluster: The target cluster to migrate the VM to
        """
        try:
            if self.dry_run:
                logger.info(f"[DRY-RUN] Would migrate VM {vm.name} to cluster {target_cluster}")
                return

            # Fetch the target cluster object
            clusters = self.service_instance.content.viewManager.CreateContainerView(
                self.service_instance.content.rootFolder, [vim.ClusterComputeResource], True
            )
            target_cluster_obj = None
            for cluster in clusters.view:
                if cluster.name == target_cluster:
                    target_cluster_obj = cluster
                    break

            if not target_cluster_obj:
                logger.error(f"Target cluster {target_cluster} not found.")
                return

            # Start the migration using vMotion
            logger.info(f"Migrating VM {vm.name} to cluster {target_cluster}...")
            task = vm.Relocate(vm.config, vim.VirtualMachineMovePriority.defaultPriority, target_cluster_obj)
            task_result = task.info.state

            if task_result == vim.TaskInfo.State.success:
                logger.success(f"VM {vm.name} successfully migrated to cluster {target_cluster}.")
            else:
                logger.error(f"VM {vm.name} migration to cluster {target_cluster} failed.")

        except Exception as e:
            logger.error(f"Error during migration of VM {vm.name}: {e}")

    def perform_migrations(self):
        """
        Iterates over each VM in the cluster distribution and migrates them
        based on the anti-affinity rule
        """
        for cluster, vms in self.vm_to_cluster_map.items():
            for vm in vms:
                self._migrate_vm(vm, cluster)

    def dry_run(self):
        """
        Executes a dry-run of the migration process without making any actual changes
        """
        logger.info("[DRY-RUN] Migration dry-run: no actual migration will be performed.")
        self.perform_migrations()

    def run(self):
        """
        Executes the migration process (or dry-run) based on the --dry-run flag
        """
        if self.dry_run:
            self.dry_run()
        else:
            self.perform_migrations()

