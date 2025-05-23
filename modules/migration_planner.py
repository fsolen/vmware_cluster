import logging

logger = logging.getLogger('fdrs')

class MigrationManager:
    def __init__(self, cluster_state, constraint_manager, aggressiveness=3):
        self.cluster_state = cluster_state
        self.constraint_manager = constraint_manager
        self.aggressiveness = aggressiveness

    def plan_migrations(self):
        """
        Main function to decide which VMs should move where.
        Returns a list of (VM, TargetHost) tuples.
        """
        logger.info("[MigrationPlanner] Planning migrations...")
        migrations = []

        imbalance_hosts = self._detect_imbalanced_hosts()

        # Handle Anti-Affinity violations first
        anti_affinity_violations = self.constraint_manager.validate_anti_affinity()
        for vm_name in anti_affinity_violations:
            vm = self.cluster_state.get_vm_by_name(vm_name)
            if hasattr(vm, 'config') and getattr(vm.config, 'template', False):
                logger.info(f"[MigrationPlanner] Skipping template VM '{vm.name}' in planning phase")
                continue
            preferred_host = self.constraint_manager.get_preferred_host_for_vm(vm)
            if preferred_host:
                migrations.append((vm, preferred_host))
                logger.info(f"[MigrationPlanner] Anti-Affinity fix planned: Move '{vm.name}' ➔ '{preferred_host.name}'")

        # Handle Load imbalance
        for host in imbalance_hosts:
            overloaded_vms = self._select_vms_to_move(host)
            for vm in overloaded_vms:
                if hasattr(vm, 'config') and getattr(vm.config, 'template', False):
                    logger.info(f"[MigrationPlanner] Skipping template VM '{vm.name}' in planning phase")
                    continue
                target_host = self._find_better_host(vm, current_host=host)
                if target_host:
                    migrations.append((vm, target_host))
                    logger.info(f"[MigrationPlanner] Load fix planned: Move '{vm.name}' from '{host.name}' ➔ '{target_host.name}'")
        
        if not migrations:
            logger.info("[MigrationPlanner] No migrations needed. Cluster is healthy.")

        return migrations

    def _detect_imbalanced_hosts(self):
        """
        Find hosts that are overloaded based on CPU, Memory, Disk IO, Network IO.
        Aggressiveness factor (1-5) controls sensitivity.
        """
        overloaded_hosts = []
        logger.info("[MigrationPlanner] Detecting overloaded hosts...")
        for host in self.cluster_state.hosts:
            metrics = self.cluster_state.host_metrics.get(host.name, {})
            if self._is_overloaded(metrics):
                overloaded_hosts.append(host)
                logger.debug(f"[MigrationPlanner] Host '{host.name}' is overloaded.")

        return overloaded_hosts

    def _is_overloaded(self, metrics):
        """
        Determine if a host is overloaded.
        Thresholds become stricter with higher aggressiveness.
        """
        cpu_threshold = 70 - (self.aggressiveness * 5)  # eg. aggressiveness 3 => 55%
        mem_threshold = 75 - (self.aggressiveness * 5)
        disk_threshold = 80 - (self.aggressiveness * 5)
        net_threshold = 80 - (self.aggressiveness * 5)

        return (
            metrics.get('cpu_usage', 0) > cpu_threshold or
            metrics.get('memory_usage', 0) > mem_threshold or
            metrics.get('disk_io_usage', 0) > disk_threshold or
            metrics.get('network_io_usage', 0) > net_threshold
        )

    def _select_vms_to_move(self, host):
        """
        From an overloaded host, pick a few VMs that are heavy (CPU, Memory, IO).
        """
        # Get VMs running on this host
        vms_on_host = [vm for vm in self.cluster_state.vms if self.cluster_state.get_host_of_vm(vm) == host.name]
        heavy_vms = sorted(
            vms_on_host,
            key=lambda vm: (
                getattr(vm, 'cpu_usage', 0) + getattr(vm, 'memory_usage', 0) + getattr(vm, 'disk_io_usage', 0) + getattr(vm, 'network_io_usage', 0)
            ),
            reverse=True
        )
        # Pick top 1-3 heavy VMs depending on aggressiveness
        count = min(self.aggressiveness, len(heavy_vms))
        return heavy_vms[:count]

    def _find_better_host(self, vm, current_host):
        """
        Search for a better host for the given VM.
        """
        candidates = []

        for host in self.cluster_state.hosts:
            if host.name == current_host.name:
                continue  # Skip current host
            if self._would_fit(vm, host):
                score = self._score_host_for_vm(vm, host)
                candidates.append((score, host))

        if not candidates:
            logger.warning(f"[MigrationPlanner] No better host found for VM '{vm.name}'.")
            return None

        # Pick host with best score
        candidates.sort(reverse=True)
        best_host = candidates[0][1]
        return best_host

    def _would_fit(self, vm, host):
        metrics = self.cluster_state.host_metrics.get(host.name, {})
        projected_cpu = metrics.get('cpu_usage', 0) + getattr(vm, 'cpu_usage', 0)
        projected_mem = metrics.get('memory_usage', 0) + getattr(vm, 'memory_usage', 0)
        projected_disk = metrics.get('disk_io_usage', 0) + getattr(vm, 'disk_io_usage', 0)
        projected_net = metrics.get('network_io_usage', 0) + getattr(vm, 'network_io_usage', 0)
        return (projected_cpu < 90 and projected_mem < 90 and projected_disk < 90 and projected_net < 90)

    def _score_host_for_vm(self, vm, host):
        metrics = self.cluster_state.host_metrics.get(host.name, {})
        cpu_score = 100 - metrics.get('cpu_usage', 0)
        mem_score = 100 - metrics.get('memory_usage', 0)
        disk_score = 100 - metrics.get('disk_io_usage', 0)
        net_score = 100 - metrics.get('network_io_usage', 0)
        return cpu_score + mem_score + disk_score + net_score
