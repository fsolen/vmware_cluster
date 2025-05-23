import logging
import time
from pyVmomi import vim

logger = logging.getLogger('fdrs')

class Scheduler:
    def __init__(self, connection_manager, dry_run=False):
        self.connection_manager = connection_manager
        self.dry_run = dry_run
        self.si = connection_manager.service_instance

    def execute_migrations(self, migrations):
        """
        Perform or simulate the VM migrations.
        """
        if not migrations:
            logger.info("[Scheduler] No migrations to perform.")
            return

        mode = "DRY-RUN" if self.dry_run else "REAL"
        logger.info(f"[Scheduler] Executing {len(migrations)} planned migrations. Mode: {mode}")

        for vm, target_host in migrations:
            try:
                if self.dry_run:
                    logger.info(f"[DRY-RUN] Would migrate VM '{vm.name}' ➔ Host '{target_host.name}'")
                else:
                    self._migrate_vm(vm, target_host)
            except Exception as e:
                logger.error(f"[Scheduler] Failed to migrate VM '{vm.name}': {str(e)}")

    def _migrate_vm(self, vm, target_host):
        """
        Actually migrate a VM to another host using vMotion.
        """
        logger.info(f"[Scheduler] Starting migration of VM '{vm.name}' ➔ '{target_host.name}'")

        relocate_spec = vim.vm.RelocateSpec()
        relocate_spec.host = target_host.ref  # Host reference
        relocate_spec.pool = target_host.resource_pool  # Optional: Assign resource pool

        task = vm.ref.Relocate(relocate_spec)

        self._wait_for_task(task, f"Migrating {vm.name}")

        logger.info(f"[Scheduler] Migration of VM '{vm.name}' to '{target_host.name}' completed successfully.")

    def _wait_for_task(self, task, action_name):
        """
        Wait for a vCenter task to complete.
        """
        logger.debug(f"[Scheduler] Waiting for task: {action_name}...")
        while task.info.state == 'running':
            time.sleep(1)

        if task.info.state == 'success':
            logger.debug(f"[Scheduler] Task '{action_name}' completed successfully.")
        else:
            raise Exception(f"Task '{action_name}' failed: {task.info.error}")
